import logging
import subprocess
import sys
import time
from pathlib import Path
from src.main import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Orchestrator] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def execute_dbt_command(command: list[str], dbt_project_dir: Path) -> None:
    """Ejecuta comandos de dbt mediante subprocess capturando stdout y códigos de salida."""
    logger.info(f"Iniciando subproceso dbt: {' '.join(command)}")
    start_time = time.time()

    process = subprocess.run(
        command,
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )

    elapsed_time = round(time.time() - start_time, 2)

    if process.returncode != 0:
        logger.error(
            f"Fallo en la ejecución de dbt (Código {process.returncode}) en {elapsed_time}s:\n{process.stderr}"
        )
        raise RuntimeError(
            f"Pipeline abortado por fallo en dbt: {process.stderr.strip()}"
        )

    logger.info(f"Comando dbt completado exitosamente en {elapsed_time}s.")


def run_end_to_end() -> None:
    """Orquestador principal que ejecuta la extracción, validación, transformación y tests."""
    logger.info(">>> INICIANDO CICLO END-TO-END DE DATOS <<<")
    pipeline_start = time.time()
    dbt_dir = Path("/workspace/analytics_dbt")

    try:
        # Fase 1: Ingestión con streaming de memoria constante y filtrado DLQ
        logger.info(
            "Fase 1/3: Ingesta de eventos, validación de contratos y persistencia Parquet..."
        )
        run_pipeline(
            total_records=5000, batch_size=1000
        )  # Ejecuta ingesta y cuarentena

        # Fase 2: Transformaciones dbt (Staging View & Incremental Marts)
        logger.info(
            "Fase 2/3: Ejecutando transformaciones analíticas en DuckDB..."
        )
        execute_dbt_command(
            ["dbt", "run", "--profiles-dir", "."], dbt_project_dir=dbt_dir
        )

        # Fase 3: Pruebas de calidad y contratos de datos
        logger.info(
            "Fase 3/3: Verificando contratos de datos y suites de pruebas..."
        )
        execute_dbt_command(
            ["dbt", "test", "--profiles-dir", "."], dbt_project_dir=dbt_dir
        )

        total_duration = round(time.time() - pipeline_start, 2)
        logger.info(
            f">>> PIPELINE FINALIZADO CON ÉXITO EN {total_duration}s <<<"
        )
        sys.exit(0)

    except Exception as exc:
        logger.critical(
            f"Fallo crítico en el pipeline de datos: {exc}", exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    run_end_to_end()