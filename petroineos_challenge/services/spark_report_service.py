from datetime import date, datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from petroineos_challenge.services.power_service import PowerService
from petroineos_challenge.utils.parser import create_period_map

spark = SparkSession.builder.appName("trade_volumes_aggregation").getOrCreate()


class SparkReportService:
    def __init__(self, report_date: date, report_location: str):
        self.report_date = report_date
        self.report_location = report_location
        self.power_service = PowerService(self.report_date)

    def trade_aggregates(self):
        trades = self.power_service.get_trades()
        trades_dict = [rec.dict() for rec in trades]
        df = spark.createDataFrame(Row(**x) for x in trades_dict)
        df_agg = df.groupBy(["trade_date", "period"]).sum("volume")
        return df_agg

    @staticmethod
    def get_lookup_period_maps():
        period_maps = [{"period": key, "local_time": val} for key, val in create_period_map().items()]
        period_map_df = spark.createDataFrame(Row(**x) for x in period_maps)
        return period_map_df

    def generate_intraday_report(self):
        agg_df = self.trade_aggregates()
        period_map_df = self.get_lookup_period_maps()
        rs_df = agg_df.join(period_map_df, (agg_df.period == period_map_df.period)).select(
            period_map_df['local_time'].alias('Local Time'), agg_df['sum(volume)'].alias('Volume')).orderBy(
            agg_df['period'])
        return rs_df

    def create_report(self):
        report_name = f"PowerPosition_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        rs_df = self.generate_intraday_report()
        rs_df.coalesce(1).write.option("header", True).csv(self.report_location + "/" + report_name)
