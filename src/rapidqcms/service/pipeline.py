"""Pipeline orchestrator: run QC modules and write gate files.

Thin glue layer between the file watcher (AcquisitionListener) and the
QC module registry. The watcher calls run_qc() once a new file is stable;
this module loads the registry, runs each module, and writes the gate file
that downstream processes (FragPipe, CZB-MAP) watch for.
"""

import logging
from pathlib import Path

from .events.gating import write_gate_file
from ..qc.base import QCResult, QCStatus
from ..qc.registry import load_registry

log = logging.getLogger(__name__)


def run_qc(
    input_path: Path,
    context: dict,
    stage: str,
    config_path: Path | None = None,
) -> list[QCResult]:
    """Run all enabled QC modules for the given input file.

    Args:
        input_path:  Path to the input file (mzML).
        context:     Dict of shared data passed to every module.
                     Keys vary by module — see each module's docstring.
        stage:       QC stage identifier written into gate files (e.g. "pre_search").
        config_path: Optional path to qc_modules.toml.  Uses the package
                     default when omitted.

    Returns:
        List of QCResult, one per module that ran (may be empty if all
        modules are disabled).
    """
    kwargs = {"config_path": config_path} if config_path else {}
    registry = load_registry(**kwargs)

    experiment_type = context.get("experiment_type")

    results: list[QCResult] = []
    for module_name, module in registry.items():
        m_stage = module.config.get("stage")
        m_exp   = module.config.get("experiment_type")
        # Skip modules that declare a stage that doesn't match
        if m_stage is not None and m_stage != stage:
            log.debug("Skipping %s (stage=%s, current=%s)", module_name, m_stage, stage)
            continue
        # Skip modules that declare an experiment_type that doesn't match
        if m_exp is not None and experiment_type is not None and m_exp != experiment_type:
            log.debug("Skipping %s (exp_type=%s, current=%s)", module_name, m_exp, experiment_type)
            continue
        log.info("Running %s QC on %s", module_name, input_path.name)
        try:
            result = module.analyze(input_path, context)
        except Exception:
            log.exception("Module %s failed on %s", module_name, input_path.name)
            result = QCResult(
                status=QCStatus.FAIL,
                module=module_name,
                message=f"Module raised an exception — see logs",
            )
        results.append(result)
        log.info(
            "%s → %s", module_name, result.status.value
        )

    # Write a single gate file representing the worst outcome across all modules
    worst = _worst_status(results) if results else QCStatus.PASS
    gate_result = QCResult(
        status=worst,
        module="pipeline",
        metrics={
            "modules_run": len(results),
            "per_module": {r.module: r.status.value for r in results},
        },
    )
    gate_path = write_gate_file(input_path, gate_result, stage)
    log.info("Gate file written: %s", gate_path)

    return results


def _worst_status(results: list[QCResult]) -> QCStatus:
    """Return the most severe status across a list of results."""
    if any(r.status == QCStatus.FAIL for r in results):
        return QCStatus.FAIL
    if any(r.status == QCStatus.WARN for r in results):
        return QCStatus.WARN
    return QCStatus.PASS
