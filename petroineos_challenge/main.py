from datetime import datetime
from pathlib import PurePath

import click

from petroineos_challenge.services.intraday_report import IntraDayReportService

# from petroineos_challenge.services.spark_report_service import SparkReportService

BASE_PATH = PurePath(__file__).parent
REPORTS_PATH = BASE_PATH / "reports"


@click.group()
def cli() -> None:
    click.echo("Execute Power Trade Report from command line. Options are as below")


@click.command()
@click.option(
    "--report_date",
    type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
    required=True,
    help="Report Date for which PowerTrade Report to be created"
)
@click.option(
    "--report_path",
    type=str,
    required=False,
    default=REPORTS_PATH,
    help="Location where the report csv to be placed"
)
def run_report(report_date, report_path):
    rpt_service = IntraDayReportService(report_date, report_path)
    rpt_service.create_report()

    # To run Spark Job enable below
    # spark_rpt_service = SparkReportService(report_date, report_path)
    # spark_rpt_service.create_report()


if __name__ == "__main__":
    run_report()
