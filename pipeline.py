import subprocess
import sys
import os
from utils.logger import get_logger

logger = get_logger("pipeline")


def run_step(nombre: str, comando: list, cwd: str = None):
    """Ejecuta un paso del pipeline y loguea el resultado."""
    logger.info(f"Iniciando: {nombre}")
    try:
        result = subprocess.run(
            comando,
            cwd=cwd,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logger.error(f"{nombre} falló:\n{result.stderr}")
            sys.exit(1)
        logger.info(f"{nombre} completado exitosamente")
    except Exception as e:
        logger.error(f"{nombre} error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("Iniciando pipeline Olist E-Commerce")
    logger.info("=" * 50)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DBT_DIR = os.path.join(BASE_DIR, "dbt_olist")

    # Paso 1 — Ingesta y validaciones
    run_step(
        "Ingesta de datos",
        [sys.executable, "ingestion/load_data.py"]
    )

    # Paso 2 — dbt staging
    run_step(
        "dbt staging",
        ["dbt", "run", "--select", "staging"],
        cwd=DBT_DIR
    )

    # Paso 3 — dbt tests
    run_step(
        "dbt tests",
        ["dbt", "test", "--select", "staging"],
        cwd=DBT_DIR
    )

    # Paso 4 — dbt marts
    run_step(
        "dbt marts",
        ["dbt", "run", "--select", "marts"],
        cwd=DBT_DIR
    )

    logger.info("=" * 50)
    logger.info("Pipeline completado exitosamente")
    logger.info("=" * 50)