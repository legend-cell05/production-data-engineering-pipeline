"""Deterministic generation of the simulated upstream systems.

One seed drives every draw, so the same configuration produces byte-identical
files on any machine. CI can therefore assert on exact record counts, and the
numbers quoted in the documentation are reproducible.

The generator deliberately produces the pathologies that make metering data
hard, each of which exercises a specific part of the pipeline:

===========================  ================================================
Pathology                    Exercised component
===========================  ================================================
register rollover            ``refresh_consumption.sql`` wrap arithmetic
meter reset                  delta flagged ``reset``, consumption NULL
offline gaps                 ``span_minutes`` and the completeness view
stuck register               ``v_meter_health.looks_stuck``
late-arriving records        the watermark's grace window
corrections re-sent later    ``DISTINCT ON ... source_updated_at DESC``
contract violations          the dead-letter queue
duplicate re-emissions       content-hash idempotency
===========================  ================================================

Everything injected is recorded in ``UpstreamDataset.anomaly_log``, which gives
the tests a known ground truth rather than an assertion about whatever the code
happened to produce.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from helios.config import Settings, get_settings
from helios.generation import reference as ref
from helios.logging_config import get_logger

logger = get_logger(__name__)

#: A record that is this late will be missed by a normal-sized grace window.
#: Generated on purpose so the limitation is demonstrable rather than
#: theoretical -- see docs/incremental.md.
_VERY_LATE_HOURS = 5.0

#: The upstream is deliberately generated up to a few hours ago rather than up
#: to this instant, so that even the latest-arriving record has an `updated_at`
#: in the past. A source that reports availability in the future is a bug, not
#: a test case.
_UPSTREAM_LAG_HOURS = 6.0


@dataclass(frozen=True)
class UpstreamDataset:
    """What the simulated upstream systems hold."""

    sites: pd.DataFrame
    meters: pd.DataFrame
    tariffs: list[dict[str, object]]
    weather: pd.DataFrame
    readings: pd.DataFrame
    anomaly_log: pd.DataFrame = field(default_factory=pd.DataFrame)

    def summary(self) -> dict[str, int]:
        return {
            "sites": len(self.sites),
            "meters": len(self.meters),
            "tariffs": len(self.tariffs),
            "weather_observations": len(self.weather),
            "readings": len(self.readings),
            "injected_anomalies": len(self.anomaly_log),
        }


# ---------------------------------------------------------------------------
# Reference entities
# ---------------------------------------------------------------------------


def _generate_sites(rng: np.random.Generator, n_sites: int, start: dt.date) -> pd.DataFrame:
    """Draw client sites across regions and sectors."""
    rows: list[dict[str, object]] = []
    for i in range(1, n_sites + 1):
        region = ref.REGIONS[int(rng.integers(len(ref.REGIONS)))]
        sector = ref.SECTORS[int(rng.integers(len(ref.SECTORS)))]
        cities = ref.CITIES[region.code]
        # Floor area is log-uniform: a few very large sites, many small ones.
        # A uniform draw would give an unrealistically flat size distribution
        # and make the per-square-metre benchmark meaningless.
        area = float(np.exp(rng.uniform(np.log(400), np.log(24_000))))
        tariff = ref.TARIFFS[int(rng.integers(len(ref.TARIFFS)))]
        rows.append(
            {
                "site_id": f"S{i:04d}",
                "site_name": (
                    f"{ref.SITE_NAME_ROOTS[int(rng.integers(len(ref.SITE_NAME_ROOTS)))]} "
                    f"{ref.SITE_NAME_SUFFIXES[int(rng.integers(len(ref.SITE_NAME_SUFFIXES)))]}"
                ),
                "city": cities[int(rng.integers(len(cities)))],
                "region_code": region.code,
                "sector": sector.name,
                "floor_area_m2": round(area, 1),
                "commissioned_on": (
                    start - dt.timedelta(days=int(rng.integers(400, 7_000)))
                ).isoformat(),
                "is_active": True,
                "tariff_id": tariff.tariff_id,
                "valid_from": (start - dt.timedelta(days=365)).isoformat(),
                "valid_to": "",
            }
        )
    return pd.DataFrame(rows)


def _generate_meters(
    rng: np.random.Generator, sites: pd.DataFrame, n_meters: int, start: dt.date
) -> pd.DataFrame:
    """Attach meters to sites, with realistic register widths and multipliers."""
    # Every site gets a main incomer first; the remaining meters are
    # sub-meters spread over the larger sites.
    assignments: list[tuple[str, str]] = []
    site_ids = sites["site_id"].tolist()
    for site_id in site_ids[: min(n_meters, len(site_ids))]:
        assignments.append((site_id, "main_incomer"))

    extra_types = [t.code for t in ref.METER_TYPES if t.code != "main_incomer"]
    while len(assignments) < n_meters:
        site_id = site_ids[int(rng.integers(len(site_ids)))]
        assignments.append((site_id, extra_types[int(rng.integers(len(extra_types)))]))

    share_by_type = {t.code: t.share for t in ref.METER_TYPES}
    rows: list[dict[str, object]] = []
    for i, (site_id, meter_type) in enumerate(assignments[:n_meters], start=1):
        # A current transformer scales the register: a large incomer counts in
        # tens or hundreds of kWh per unit. Keeping the multiplier separate is
        # what lets core reconcile against the physical device.
        multiplier = float(rng.choice([1.0, 1.0, 1.0, 1.0, 10.0, 40.0]))
        # Narrow registers on purpose for roughly a third of the meters, so a
        # rollover actually happens inside the observation window. Not narrower
        # than six digits: a register that wraps several times a day is not
        # something a utility would install, and generating one would be
        # testing an impossible case rather than a hard one.
        digits = int(rng.choice([8, 8, 7, 7, 6, 6], p=[0.30, 0.20, 0.20, 0.15, 0.10, 0.05]))
        rows.append(
            {
                "meter_id": f"M{i:05d}",
                "site_id": site_id,
                "meter_type": meter_type,
                "unit": "kWh",
                "multiplier": multiplier,
                "index_digits": digits,
                "interval_minutes": 15,
                "installed_on": (
                    start - dt.timedelta(days=int(rng.integers(200, 3_500)))
                ).isoformat(),
                "is_active": True,
                "load_share": share_by_type[meter_type],
            }
        )
    return pd.DataFrame(rows)


def _generate_tariffs(start: dt.date) -> list[dict[str, object]]:
    """The tariff catalogue, with its bands nested as the source system sends it."""
    return [
        {
            "tariff_id": t.tariff_id,
            "tariff_name": t.name,
            "supplier": t.supplier,
            "currency": "EUR",
            "standing_charge_per_day": t.standing_charge_per_day,
            "valid_from": (start - dt.timedelta(days=365)).isoformat(),
            "valid_to": "",
            "bands": [
                {
                    "band_code": b.code,
                    "hour_start": b.hour_start,
                    "hour_end": b.hour_end,
                    "price_per_kwh": b.price_per_kwh,
                }
                for b in t.bands
            ],
            "updated_at": dt.datetime.combine(
                start - dt.timedelta(days=365), dt.time(6, 0), tzinfo=dt.UTC
            ).isoformat(),
        }
        for t in ref.TARIFFS
    ]


def _generate_weather(rng: np.random.Generator, start: dt.date, end: dt.date) -> pd.DataFrame:
    """Daily temperature per station, with a seasonal cycle and day-to-day noise."""
    days = pd.date_range(start, end, freq="D")
    rows: list[dict[str, object]] = []
    for region in ref.REGIONS:
        # Seasonal sinusoid with its minimum in mid-January (day 15).
        day_of_year = days.dayofyear.to_numpy()
        seasonal = region.mean_temp_c - region.seasonal_amplitude_c * np.cos(
            2 * np.pi * (day_of_year - 15) / 365.25
        )
        # Weather is autocorrelated: a warm day is usually followed by another.
        # White noise would produce a physically absurd series.
        noise = np.zeros(len(days))
        shock = rng.normal(0, 2.4, len(days))
        for i in range(1, len(days)):
            noise[i] = 0.68 * noise[i - 1] + shock[i]
        avg = seasonal + noise
        swing = rng.uniform(4.0, 11.0, len(days))

        for day, mean_t, amplitude in zip(days, avg, swing, strict=True):
            rows.append(
                {
                    "station_id": region.station_id,
                    "station_name": region.station_name,
                    "region_code": region.code,
                    "observed_on": day.date().isoformat(),
                    "temp_min_c": round(float(mean_t - amplitude / 2), 2),
                    "temp_max_c": round(float(mean_t + amplitude / 2), 2),
                    "temp_avg_c": round(float(mean_t), 2),
                    # Weather lands the morning after the day it describes.
                    "updated_at": dt.datetime.combine(
                        day.date() + dt.timedelta(days=1), dt.time(5, 30), tzinfo=dt.UTC
                    ).isoformat(),
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


def _meter_power_series(
    rng: np.random.Generator,
    timestamps: pd.DatetimeIndex,
    sector: ref.Sector,
    area_m2: float,
    share: float,
    daily_temp: np.ndarray,
) -> np.ndarray:
    """Instantaneous power in kW for one meter, at every interval.

    Combines four effects, each multiplicative or additive as physics suggests:
    the sector's hour-of-day shape, a weekend factor, a temperature-driven
    heating/cooling term, and multiplicative noise.
    """
    hours = timestamps.hour.to_numpy()
    is_weekend = timestamps.dayofweek.to_numpy() >= 5

    peak_kw = sector.peak_w_per_m2 * area_m2 * share / 1000.0
    shape = np.asarray(sector.hourly_profile)[hours]

    # The base load is a floor, not something the profile scales: a building
    # draws it whether or not anyone is inside.
    occupancy = np.where(is_weekend, sector.weekend_factor, 1.0)
    active = np.maximum(shape * occupancy, sector.base_load_ratio)

    hdd = np.maximum(0.0, 18.0 - daily_temp)
    cdd = np.maximum(0.0, daily_temp - 18.0)
    thermal_kw = (
        (sector.hdd_sensitivity * hdd + sector.cdd_sensitivity * cdd) * area_m2 * share / 1000.0
    )

    noise = rng.normal(1.0, 0.045, len(timestamps)).clip(0.80, 1.25)
    power: np.ndarray = np.maximum(0.0, (peak_kw * active + thermal_kw) * noise)
    return power


def _generate_readings(
    rng: np.random.Generator,
    cfg: Settings,
    sites: pd.DataFrame,
    meters: pd.DataFrame,
    weather: pd.DataFrame,
    start: dt.date,
    end: dt.date,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """Produce the interval readings, with their injected pathologies."""
    now = pd.Timestamp.now(tz="UTC").floor(f"{cfg.interval_minutes}min")
    last_reading = now - pd.Timedelta(hours=_UPSTREAM_LAG_HOURS)
    timestamps = pd.date_range(
        dt.datetime.combine(start, dt.time(0, 0), tzinfo=dt.UTC),
        last_reading,
        freq=f"{cfg.interval_minutes}min",
        tz="UTC",
    )
    n_intervals = len(timestamps)
    hours_per_interval = cfg.interval_minutes / 60.0

    sector_by_name = {s.name: s for s in ref.SECTORS}
    site_lookup = sites.set_index("site_id")

    # Daily mean temperature per region, aligned onto the interval grid.
    temp_by_region: dict[str, np.ndarray] = {}
    interval_dates = pd.Series(timestamps.date, index=range(n_intervals))
    for region_code, group in weather.groupby("region_code"):
        by_day = dict(
            zip(
                pd.to_datetime(group["observed_on"]).dt.date,
                group["temp_avg_c"].astype(float),
                strict=True,
            )
        )
        temp_by_region[str(region_code)] = interval_dates.map(
            lambda d, table=by_day: table.get(d, 12.0)
        ).to_numpy(dtype=float)

    log: list[dict[str, object]] = []
    frames: list[pd.DataFrame] = []

    # Meters singled out for a specific pathology, chosen up front so the
    # counts are deterministic and assertable.
    meter_ids = meters["meter_id"].to_numpy()
    n_reset = max(1, int(len(meter_ids) * cfg.reset_rate))
    reset_meters = set(rng.choice(meter_ids, size=n_reset, replace=False))
    stuck_meters = set(rng.choice(meter_ids, size=max(1, len(meter_ids) // 30), replace=False))

    for row in meters.itertuples(index=False):
        site = site_lookup.loc[row.site_id]
        sector = sector_by_name[str(site["sector"])]
        temps = temp_by_region.get(str(site["region_code"]), np.full(n_intervals, 12.0))

        power_kw = _meter_power_series(
            rng,
            timestamps,
            sector,
            float(str(site["floor_area_m2"])),
            float(str(row.load_share)),
            temps,
        )
        energy_kwh = power_kw * hours_per_interval

        # A stuck register stops counting for a contiguous stretch but keeps
        # reporting: the hardest failure to notice, because the data looks fine.
        if row.meter_id in stuck_meters:
            begin = int(rng.integers(0, max(1, n_intervals - n_intervals // 4)))
            length = int(n_intervals // 5)
            energy_kwh[begin : begin + length] = 0.0
            log.append(
                {"kind": "stuck_register", "reference": str(row.meter_id), "records": length}
            )

        register_max = 10.0 ** int(str(row.index_digits))
        multiplier = float(str(row.multiplier))
        register_steps = energy_kwh / multiplier

        # Start high enough that the narrow registers wrap inside the window.
        start_register = float(rng.uniform(0.55, 0.97) * register_max)
        raw_index = start_register + np.cumsum(register_steps)

        if row.meter_id in reset_meters:
            at = int(rng.integers(n_intervals // 4, n_intervals))
            raw_index[at:] = raw_index[at:] - raw_index[at] + float(rng.uniform(0, 50))
            log.append({"kind": "meter_reset", "reference": str(row.meter_id), "records": 1})

        register_value = np.round(np.mod(raw_index, register_max), 3)
        if float(raw_index.max()) >= register_max:
            log.append({"kind": "register_rollover", "reference": str(row.meter_id), "records": 1})

        frames.append(
            pd.DataFrame(
                {
                    "meter_id": row.meter_id,
                    "reading_ts": timestamps,
                    "register_value": register_value,
                    "quality_flag": "measured",
                }
            )
        )

    readings = pd.concat(frames, ignore_index=True)

    # --- Availability: when did the record reach the upstream API? ----------
    # Most records are available within minutes. A meter that was offline
    # buffers and flushes hours later -- which is exactly what the watermark's
    # grace window exists for.
    n = len(readings)
    lag_minutes = rng.gamma(shape=2.0, scale=1.4, size=n) + 0.5

    buffered = rng.random(n) < 0.02
    lag_minutes[buffered] = rng.uniform(90, 240, int(buffered.sum()))
    log.append({"kind": "late_arrival", "reference": "buffered", "records": int(buffered.sum())})

    very_late = rng.random(n) < 0.0015
    lag_minutes[very_late] = _VERY_LATE_HOURS * 60
    log.append(
        {
            "kind": "very_late_arrival",
            "reference": f">{_VERY_LATE_HOURS:.0f}h",
            "records": int(very_late.sum()),
        }
    )

    readings["updated_at"] = readings["reading_ts"] + pd.to_timedelta(lag_minutes, unit="m")

    # --- Gaps: intervals the meter never reported ---------------------------
    keep = rng.random(n) >= cfg.gap_rate
    dropped = int((~keep).sum())
    readings = readings.loc[keep].reset_index(drop=True)
    log.append({"kind": "missing_interval", "reference": "random", "records": dropped})

    # A handful of multi-hour outages, which look different from scattered
    # single misses and are what an operations team actually chases.
    for _ in range(3):
        meter = str(rng.choice(meter_ids))
        outage_start = timestamps[int(rng.integers(0, max(1, n_intervals - 40)))]
        outage = (readings["meter_id"] == meter) & readings["reading_ts"].between(
            outage_start, outage_start + pd.Timedelta(hours=8)
        )
        log.append({"kind": "outage", "reference": meter, "records": int(outage.sum())})
        readings = readings.loc[~outage].reset_index(drop=True)

    # --- Corrections: the same interval re-sent later with a new value ------
    n = len(readings)
    corrected_idx = rng.choice(n, size=max(1, int(n * 0.003)), replace=False)
    corrections = readings.iloc[corrected_idx].copy()
    # A correction nudges the register by a few units, not by a percentage.
    # A cumulative index sits in the millions; a 0.4% "correction" of it would
    # imply gigawatts over a quarter of an hour, which is not a correction, it
    # is a different kind of corruption.
    corrections["register_value"] = np.round(
        corrections["register_value"].to_numpy() + rng.uniform(-3.0, 3.0, len(corrections)), 3
    ).clip(min=0.0)
    corrections["quality_flag"] = "substituted"
    corrections["updated_at"] = corrections["updated_at"] + pd.Timedelta(hours=3)
    log.append({"kind": "correction", "reference": "resent", "records": len(corrections)})

    # --- Exact re-emissions: the source sends the same payload twice --------
    duplicate_idx = rng.choice(n, size=max(1, int(n * 0.004)), replace=False)
    duplicates = readings.iloc[duplicate_idx].copy()
    log.append({"kind": "exact_duplicate", "reference": "re-emitted", "records": len(duplicates)})

    readings = pd.concat([readings, corrections, duplicates], ignore_index=True)

    # --- Corrupted magnitudes: values that pass the contract but are absurd -
    # A transmission fault that scales a register. The contract cannot catch it
    # -- the value is a positive number within range -- so it has to be caught
    # downstream by the plausibility rule, which is exactly the point.
    spike_idx = rng.choice(len(readings), size=15, replace=False)
    readings.loc[readings.index[spike_idx], "register_value"] = np.round(
        readings.iloc[spike_idx]["register_value"].to_numpy() * 1.6, 3
    )
    log.append({"kind": "corrupted_magnitude", "reference": "transmission", "records": 15})

    # --- Contract violations: records the pipeline must refuse --------------
    readings, violation_log = _inject_contract_violations(rng, readings, meter_ids)
    log.extend(violation_log)

    readings["reading_id"] = [f"R{i:09d}" for i in range(len(readings))]

    # The API paginates on `updated_at`, so the store must be ordered by it.
    readings = readings.sort_values(["updated_at", "meter_id", "reading_ts"]).reset_index(drop=True)
    return readings, log


def _inject_contract_violations(
    rng: np.random.Generator, readings: pd.DataFrame, meter_ids: np.ndarray
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """Add records that violate the schema contract and must be dead-lettered.

    Each maps to one contract rule, so the DLQ report names a real cause.
    """
    log: list[dict[str, object]] = []
    n = len(readings)
    bad: list[dict[str, object]] = []

    def _template() -> dict[str, object]:
        row = readings.iloc[int(rng.integers(n))]
        return {
            "meter_id": row["meter_id"],
            "reading_ts": row["reading_ts"],
            "register_value": float(row["register_value"]),
            "quality_flag": "measured",
            "updated_at": row["updated_at"],
        }

    for _ in range(12):
        record = _template()
        record["register_value"] = -abs(float(str(record["register_value"])))
        bad.append(record)
    log.append({"kind": "violation_negative_register", "reference": "contract", "records": 12})

    for _ in range(8):
        record = _template()
        record["meter_id"] = ""
        bad.append(record)
    log.append({"kind": "violation_missing_meter_id", "reference": "contract", "records": 8})

    for _ in range(6):
        record = _template()
        record["quality_flag"] = "teleported"
        bad.append(record)
    log.append({"kind": "violation_unknown_quality_flag", "reference": "contract", "records": 6})

    for _ in range(5):
        record = _template()
        record["meter_id"] = "M99999"  # a meter that does not exist
        bad.append(record)
    log.append({"kind": "violation_unknown_meter", "reference": "contract", "records": 5})

    return pd.concat([readings, pd.DataFrame(bad)], ignore_index=True), log


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_upstream(settings: Settings | None = None) -> UpstreamDataset:
    """Generate everything the simulated upstream systems hold."""
    cfg = settings or get_settings()
    rng = np.random.default_rng(cfg.random_seed)

    end = dt.date.today()
    start = end - dt.timedelta(days=cfg.history_days - 1)

    logger.info(
        "generating upstream data",
        extra={
            "seed": cfg.random_seed,
            "sites": cfg.n_sites,
            "meters": cfg.n_meters,
            "period": f"{start} -> {end}",
            "interval_minutes": cfg.interval_minutes,
        },
    )

    sites = _generate_sites(rng, cfg.n_sites, start)
    meters = _generate_meters(rng, sites, cfg.n_meters, start)
    tariffs = _generate_tariffs(start)
    weather = _generate_weather(rng, start, end)
    readings, log = _generate_readings(rng, cfg, sites, meters, weather, start, end)

    # Reference snapshots are exported nightly; give them a plausible
    # source-side timestamp so the watermark logic has something to work with.
    snapshot_at = dt.datetime.combine(end, dt.time(2, 0), tzinfo=dt.UTC).isoformat()
    sites["updated_at"] = snapshot_at
    # `load_share` drives generation but is not something a source system
    # would expose, so it never leaves this module.
    meters = meters.drop(columns=["load_share"])
    meters["updated_at"] = snapshot_at

    dataset = UpstreamDataset(
        sites=sites,
        meters=meters,
        tariffs=tariffs,
        weather=weather,
        readings=readings,
        anomaly_log=pd.DataFrame(log),
    )
    logger.info("upstream generated", extra=dataset.summary())
    return dataset


def write_upstream(dataset: UpstreamDataset, settings: Settings | None = None) -> dict[str, Path]:
    """Write the dataset out as the upstream systems would expose it.

    Reference data lands as CSV and JSON in ``data/landing`` -- the nightly
    export drop. Telemetry goes to ``data/upstream`` as newline-delimited JSON,
    which is what the API reads and paginates over; it is deliberately not in
    the landing directory, because the pipeline must reach it over HTTP.
    """
    cfg = settings or get_settings()
    cfg.ensure_directories()

    written: dict[str, Path] = {}

    sites_path = cfg.landing_dir / "sites.csv"
    dataset.sites.to_csv(sites_path, index=False)
    written["sites"] = sites_path

    meters_path = cfg.landing_dir / "meters.csv"
    dataset.meters.to_csv(meters_path, index=False)
    written["meters"] = meters_path

    weather_path = cfg.landing_dir / "weather.csv"
    dataset.weather.to_csv(weather_path, index=False)
    written["weather"] = weather_path

    tariffs_path = cfg.landing_dir / "tariffs.json"
    tariffs_path.write_text(
        json.dumps({"tariffs": dataset.tariffs}, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    written["tariffs"] = tariffs_path

    readings_path = cfg.upstream_dir / "readings.ndjson"
    export = dataset.readings.copy()
    export["reading_ts"] = pd.to_datetime(export["reading_ts"], utc=True, errors="coerce")
    export["updated_at"] = pd.to_datetime(export["updated_at"], utc=True, errors="coerce")
    export["reading_ts"] = export["reading_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    export["updated_at"] = export["updated_at"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    export.to_json(readings_path, orient="records", lines=True)
    written["readings"] = readings_path

    log_path = cfg.upstream_dir / "_anomaly_log.csv"
    dataset.anomaly_log.to_csv(log_path, index=False)
    written["anomaly_log"] = log_path

    for name, path in written.items():
        logger.info("upstream file written", extra={"dataset": name, "file": path.name})
    return written
