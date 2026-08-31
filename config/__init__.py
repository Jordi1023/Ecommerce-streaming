import os
import yaml
from pathlib import Path

CONFIG_DIR = Path(__file__).parent
CONFIG_PATH = CONFIG_DIR / "pipeline_config.yaml"

def load_config():
    """Carga el archivo de configuracion centralizado del pipeline."""
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

config = load_config()