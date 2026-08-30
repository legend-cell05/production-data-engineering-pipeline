"""Generation of the simulated upstream systems.

Everything here is **fabricated**. "Helios Energy" does not exist; neither do
its client sites, meters or consumption figures.

The dataset is shaped to be *plausible* rather than uniform -- sector-specific
load profiles, weekend effects, temperature-driven heating, and the specific
pathologies that make real metering data difficult: register rollovers, meter
resets, offline gaps, stuck registers, late-arriving records and corrections
re-sent hours later.

Those pathologies are the point. A pipeline demonstrated on clean data
demonstrates nothing.
"""

from helios.generation.upstream import UpstreamDataset, generate_upstream, write_upstream

__all__ = ["UpstreamDataset", "generate_upstream", "write_upstream"]
