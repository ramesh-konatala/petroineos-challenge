from datetime import date, datetime

from petroineos_challenge.services.power_service import PowerService
from petroineos_challenge.utils.parser import create_period_map, dict_to_csv

from petroineos_challenge.utils.logger import get_logger

logger = get_logger(__name__)


class IntraDayReportService:
    def __init__(self, report_date: date, report_location: str):
        self.report_date = report_date
        self.report_location = report_location
        self.power_service = PowerService(self.report_date)

    def trade_aggregates(self):
        totals = {}
        trades = self.power_service.get_trades()
        logger.info(f"Total trade position records extracted:{len(trades)}")
        for trade in trades:
            totals[trade.period] = totals.get(trade.period, 0) + trade.volume
        return totals

    def generate_intraday_report(self):
        hourly_aggregates = self.trade_aggregates()
        period_maps = create_period_map()
        report_data = [{"Local Time": period_maps[key], "Volume": val} for key, val in hourly_aggregates.items()]
        logger.info(f"Total trade position aggregated records:{len(report_data)}")
        return report_data

    def create_report(self):
        logger.info(f"Intra-day report for {self.report_date.isoformat()} started at:{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}")
        rpt_data = self.generate_intraday_report()
        report_full_path = self.report_location + "/" + f"PowerPosition_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        dict_to_csv(rpt_data, report_full_path)
        logger.info(f"Intra-day report for created successfully at:{report_full_path}")
        logger.info(f"Intra-day report for {self.report_date.isoformat()} completed at:{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}")






