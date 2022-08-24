from datetime import date, datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from petroineos_challenge.services.power_service import PowerService
from petroineos_challenge.utils.parser import create_period_map
from petroineos_challenge.utils.logger import get_logger

spark = SparkSession.builder.appName("trade_volumes_aggregation").getOrCreate()

logger = get_logger(__name__)


class SparkReportService:
    def __init__(self, report_date: date, report_location: str):
        self.report_date = report_date
        self.report_location = report_location
        self.power_service = PowerService(self.report_date)

    def trade_aggregates(self):
        trades = self.power_service.get_trades()
        logger.info(f"Total trade position records extracted:{len(trades)}")
        trades_dict = [rec.dict() for rec in trades]
        df = spark.createDataFrame(Row(**x) for x in trades_dict)
        df_agg = df.groupBy(["trade_date", "period"]).sum("volume")
        return df_agg

    @staticmethod
    def get_lookup_period_maps():
        period_maps = [{"period": key, "local_time": val} for key, val in create_period_map().items()]
        period_map_df = spark.createDataFrame(Row(**x) for x in period_maps)
        logger.info("period-to-time mapping lookup dataframe created successfully")
        return period_map_df

    def generate_intraday_report(self):
        agg_df = self.trade_aggregates()
        period_map_df = self.get_lookup_period_maps()
        rs_df = agg_df.join(period_map_df, (agg_df.period == period_map_df.period)).select(
            period_map_df['local_time'].alias('Local Time'), agg_df['sum(volume)'].alias('Volume')).orderBy(
            agg_df['period'])
        logger.info(f"Total trade position aggregated records:{len(rs_df)}")
        return rs_df

    def create_report(self):
        logger.info(
            f"Intra-day report for {self.report_date.isoformat()} started at:{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}")
        report_name = f"PowerPosition_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        rs_df = self.generate_intraday_report()
        report_full_path = self.report_location + "/" + report_name
        rs_df.coalesce(1).write.option("header", True).csv(report_full_path)
        logger.info(f"Intra-day report for created successfully at:{report_full_path}")
        logger.info(
            f"Intra-day report for {self.report_date.isoformat()} completed at:{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}")
