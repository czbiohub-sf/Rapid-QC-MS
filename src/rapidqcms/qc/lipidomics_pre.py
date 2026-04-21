"""Lipidomics pre-search QC module.

Stub that applies scan-count QC to lipidomics mzML files (C18 / lipid
chromatography).  Uses the same MS1/MS2 counting logic as proteomics_pre
with lipidomics-appropriate defaults — lipidomics DDA runs typically produce
fewer MS2 scans and a lower MS2/MS1 ratio than proteomics.

Future work: add lipid-specific IS detection once a lipidomics IS library
is configured in qc_config.yaml.
"""

from .proteomics_pre import ProteomicsPreSearchQCModule


class LipidomicsPreSearchQCModule(ProteomicsPreSearchQCModule):
    """Pre-search scan-count QC for lipidomics mzML files."""

    @property
    def name(self) -> str:
        return "lipidomics_pre"
