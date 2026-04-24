import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_retailstream"


def run_step(step_name: str, command: list[str], workdir: Path | None = None) -> None:
    print(f"Running: {step_name}")

    result = subprocess.run(
        command,
        cwd=workdir or PROJECT_ROOT,
        check=False,
    )

    if result.returncode != 0:
        print(f"FAILED at {step_name}")
        sys.exit(result.returncode)


def main() -> None:
    steps = [
        (
            "create_bronze_tables",
            ["python", "bronze/create_bronze_tables.py"],
            PROJECT_ROOT,
        ),
        (
            "load_to_bronze",
            ["python", "ingestion/load_to_bronze.py"],
            PROJECT_ROOT,
        ),
        (
            "dbt_run",
            ["dbt", "run"],
            DBT_PROJECT_DIR,
        ),
        (
            "dbt_test",
            ["dbt", "test"],
            DBT_PROJECT_DIR,
        ),
        (
            "quality_checks",
            ["python", "quality/quality_checks.py"],
            PROJECT_ROOT,
        ),
        (
            "kpi_analysis",
            ["python", "pipeline/kpi_analysis.py"],
            PROJECT_ROOT,
        ),
    ]

    for step_name, command, workdir in steps:
        run_step(step_name, command, workdir)

    print("PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
