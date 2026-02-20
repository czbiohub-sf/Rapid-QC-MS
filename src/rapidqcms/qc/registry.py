import importlib
import tomllib
from pathlib import Path

from .base import QCModule

_DEFAULT_CONFIG = Path(__file__).parent.parent / "config" / "qc_modules.toml"


def load_module_from_config(module_config: dict) -> QCModule:
    """Dynamically load a QC module class and instantiate it from its config block."""
    class_path = module_config["class"]
    module_path, class_name = class_path.rsplit(".", 1)
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls.from_config(module_config)


def load_registry(config_path: Path = _DEFAULT_CONFIG) -> dict[str, QCModule]:
    """Load all enabled QC modules from a TOML config file.

    Returns a dict mapping module name → QCModule instance.
    Modules with enabled = false are skipped.
    """
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    modules: dict[str, QCModule] = {}
    for name, module_config in config.get("modules", {}).items():
        if module_config.get("enabled", True):
            modules[name] = load_module_from_config(module_config)

    return modules
