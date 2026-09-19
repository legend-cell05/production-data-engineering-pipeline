"""Reference data for the fictional estate.

The scenario: **Helios Energy**, an invented energy-management operator that
monitors electricity meters across client sites in France. It does not exist.

Keeping the domain constants here -- load profiles, tariffs, regions -- makes
the modelling assumptions explicit and lets the generation logic stay generic.
"""

from __future__ import annotations

from typing import Final, NamedTuple


class Region(NamedTuple):
    """A geographic area with its reference weather station."""

    code: str
    name: str
    station_id: str
    station_name: str
    #: Mean annual temperature and the amplitude of the seasonal swing, used to
    #: generate a plausible temperature series.
    mean_temp_c: float
    seasonal_amplitude_c: float


REGIONS: Final[tuple[Region, ...]] = (
    Region("IDF", "Ile-de-France", "WS-IDF", "Paris-Montsouris", 12.4, 8.2),
    Region("HDF", "Hauts-de-France", "WS-HDF", "Lille-Lesquin", 11.1, 7.6),
    Region("GES", "Grand Est", "WS-GES", "Strasbourg-Entzheim", 11.0, 9.4),
    Region("ARA", "Auvergne-Rhone-Alpes", "WS-ARA", "Lyon-Bron", 12.6, 9.1),
    Region("PAC", "Provence-Alpes-Cote d'Azur", "WS-PAC", "Marseille-Marignane", 15.6, 8.0),
    Region("OCC", "Occitanie", "WS-OCC", "Toulouse-Blagnac", 14.1, 8.4),
    Region("NAQ", "Nouvelle-Aquitaine", "WS-NAQ", "Bordeaux-Merignac", 13.8, 8.1),
    Region("BRE", "Bretagne", "WS-BRE", "Rennes-Saint-Jacques", 12.2, 6.4),
)


class Sector(NamedTuple):
    """A building type and its electrical behaviour.

    ``hourly_profile`` is a 24-value shape factor applied to the site's peak
    demand. ``base_load_ratio`` is what the building draws when nobody is in
    it -- the floor under the profile.
    """

    name: str
    hourly_profile: tuple[float, ...]
    weekend_factor: float
    base_load_ratio: float
    #: Peak electrical demand per square metre, in watts.
    peak_w_per_m2: float
    #: Additional watts per square metre per heating degree day. Zero for a
    #: site that is not electrically heated.
    hdd_sensitivity: float
    cdd_sensitivity: float


#: Profiles are normalised so the daily maximum is 1.0.
SECTORS: Final[tuple[Sector, ...]] = (
    Sector(
        "Office",
        (
            0.22,
            0.20,
            0.19,
            0.19,
            0.20,
            0.24,
            0.38,
            0.62,
            0.86,
            0.96,
            1.00,
            0.98,
            0.92,
            0.95,
            0.99,
            0.97,
            0.90,
            0.74,
            0.52,
            0.38,
            0.30,
            0.26,
            0.24,
            0.23,
        ),
        weekend_factor=0.28,
        base_load_ratio=0.19,
        peak_w_per_m2=38.0,
        hdd_sensitivity=0.55,
        cdd_sensitivity=0.70,
    ),
    Sector(
        "Retail",
        (
            0.24,
            0.22,
            0.21,
            0.21,
            0.22,
            0.26,
            0.34,
            0.52,
            0.78,
            0.92,
            0.97,
            1.00,
            0.98,
            0.96,
            0.95,
            0.96,
            0.97,
            0.94,
            0.86,
            0.70,
            0.46,
            0.32,
            0.27,
            0.25,
        ),
        weekend_factor=0.92,
        base_load_ratio=0.21,
        peak_w_per_m2=62.0,
        hdd_sensitivity=0.40,
        cdd_sensitivity=1.10,
    ),
    Sector(
        "Industrial",
        (
            0.62,
            0.60,
            0.59,
            0.59,
            0.62,
            0.72,
            0.88,
            0.96,
            1.00,
            0.99,
            0.98,
            0.94,
            0.86,
            0.95,
            0.99,
            0.98,
            0.94,
            0.88,
            0.80,
            0.74,
            0.70,
            0.67,
            0.65,
            0.63,
        ),
        weekend_factor=0.45,
        base_load_ratio=0.58,
        peak_w_per_m2=95.0,
        hdd_sensitivity=0.30,
        cdd_sensitivity=0.35,
    ),
    Sector(
        "Logistics",
        (
            0.70,
            0.72,
            0.74,
            0.76,
            0.80,
            0.86,
            0.92,
            0.95,
            0.94,
            0.90,
            0.86,
            0.84,
            0.82,
            0.84,
            0.86,
            0.88,
            0.92,
            0.96,
            1.00,
            0.98,
            0.92,
            0.84,
            0.78,
            0.73,
        ),
        weekend_factor=0.66,
        base_load_ratio=0.68,
        peak_w_per_m2=28.0,
        hdd_sensitivity=0.22,
        cdd_sensitivity=0.90,
    ),
    Sector(
        # Near-flat by design: a data centre that varies much is a data centre
        # with a problem.
        "Data centre",
        (
            0.96,
            0.96,
            0.95,
            0.95,
            0.95,
            0.96,
            0.97,
            0.98,
            0.99,
            1.00,
            1.00,
            1.00,
            0.99,
            1.00,
            1.00,
            1.00,
            0.99,
            0.98,
            0.98,
            0.97,
            0.97,
            0.96,
            0.96,
            0.96,
        ),
        weekend_factor=0.98,
        base_load_ratio=0.94,
        peak_w_per_m2=780.0,
        hdd_sensitivity=0.0,
        cdd_sensitivity=2.40,
    ),
    Sector(
        "Healthcare",
        (
            0.58,
            0.56,
            0.55,
            0.55,
            0.57,
            0.64,
            0.78,
            0.90,
            0.97,
            1.00,
            0.99,
            0.97,
            0.94,
            0.96,
            0.97,
            0.95,
            0.92,
            0.86,
            0.78,
            0.72,
            0.68,
            0.64,
            0.61,
            0.59,
        ),
        weekend_factor=0.84,
        base_load_ratio=0.54,
        peak_w_per_m2=72.0,
        hdd_sensitivity=0.65,
        cdd_sensitivity=0.95,
    ),
    Sector(
        "Education",
        (
            0.16,
            0.15,
            0.14,
            0.14,
            0.15,
            0.20,
            0.38,
            0.70,
            0.94,
            1.00,
            0.99,
            0.94,
            0.82,
            0.92,
            0.96,
            0.90,
            0.70,
            0.44,
            0.28,
            0.22,
            0.19,
            0.18,
            0.17,
            0.16,
        ),
        weekend_factor=0.12,
        base_load_ratio=0.14,
        peak_w_per_m2=30.0,
        hdd_sensitivity=0.70,
        cdd_sensitivity=0.25,
    ),
)


class MeterType(NamedTuple):
    """A meter and the share of its site's load that it measures."""

    code: str
    share: float


#: A site's load is split across its meters. The shares are relative weights,
#: normalised per site -- a site does not necessarily have every type.
METER_TYPES: Final[tuple[MeterType, ...]] = (
    MeterType("main_incomer", 1.00),
    MeterType("hvac", 0.38),
    MeterType("lighting", 0.18),
    MeterType("process", 0.30),
    MeterType("ev_charging", 0.08),
    MeterType("submeter", 0.12),
)


class TariffBand(NamedTuple):
    code: str
    hour_start: int
    hour_end: int
    price_per_kwh: float


class Tariff(NamedTuple):
    tariff_id: str
    name: str
    supplier: str
    standing_charge_per_day: float
    bands: tuple[TariffBand, ...]


#: Three time-of-use tariffs. The bands must tile 0..24 without overlap --
#: v_cost_by_band joins on `hour >= start AND hour < end`, so a gap silently
#: drops consumption and an overlap silently doubles it. A quality check
#: verifies the tiling after every load.
TARIFFS: Final[tuple[Tariff, ...]] = (
    Tariff(
        "TRF-BASE",
        "Base flat rate",
        "Voltalia Supply",
        standing_charge_per_day=1.42,
        bands=(TariffBand("ALL", 0, 24, 0.2016),),
    ),
    Tariff(
        "TRF-HPHC",
        "Peak / off-peak",
        "Voltalia Supply",
        standing_charge_per_day=1.68,
        bands=(
            TariffBand("OFFPEAK_NIGHT", 0, 6, 0.1470),
            TariffBand("PEAK_DAY", 6, 22, 0.2280),
            TariffBand("OFFPEAK_LATE", 22, 24, 0.1470),
        ),
    ),
    Tariff(
        "TRF-TEMPO",
        "Four-band industrial",
        "Nordelec Energie",
        standing_charge_per_day=3.95,
        bands=(
            TariffBand("NIGHT", 0, 7, 0.1105),
            TariffBand("MORNING", 7, 12, 0.2450),
            TariffBand("MIDDAY", 12, 17, 0.1890),
            TariffBand("EVENING", 17, 24, 0.2710),
        ),
    ),
)

CITIES: Final[dict[str, tuple[str, ...]]] = {
    "IDF": ("Paris", "Nanterre", "Creteil", "Argenteuil", "Versailles", "Melun"),
    "HDF": ("Lille", "Amiens", "Roubaix", "Dunkerque"),
    "GES": ("Strasbourg", "Metz", "Reims", "Mulhouse"),
    "ARA": ("Lyon", "Grenoble", "Saint-Etienne", "Annecy"),
    "PAC": ("Marseille", "Nice", "Toulon", "Aix-en-Provence"),
    "OCC": ("Toulouse", "Montpellier", "Nimes", "Perpignan"),
    "NAQ": ("Bordeaux", "Pau", "Limoges", "La Rochelle"),
    "BRE": ("Rennes", "Brest", "Quimper", "Vannes"),
}

SITE_NAME_ROOTS: Final[tuple[str, ...]] = (
    "Aurore",
    "Bellevue",
    "Cristal",
    "Damier",
    "Esperance",
    "Fontanelle",
    "Genepi",
    "Horizon",
    "Iris",
    "Jonquille",
    "Kelvin",
    "Lumiere",
    "Meridien",
    "Nacelle",
    "Oriane",
    "Passerelle",
    "Quadrant",
    "Roseraie",
    "Sirius",
    "Tramontane",
    "Ulysse",
    "Verseau",
    "Wattignies",
    "Xenon",
    "Ysope",
    "Zephyr",
    "Ancolie",
    "Bastide",
    "Calypso",
    "Dolmen",
    "Eclipse",
    "Falaise",
    "Grisolles",
    "Hermine",
    "Ilot",
    "Jouvence",
    "Kaolin",
    "Lisiere",
    "Mistral",
    "Nivose",
)

SITE_NAME_SUFFIXES: Final[tuple[str, ...]] = (
    "Campus",
    "Park",
    "Centre",
    "Plateau",
    "Works",
    "Hub",
    "Court",
    "Halle",
)
