from datetime import date
from unittest.mock import MagicMock, patch

from petroineos_challenge.services.intraday_report import IntraDayReportService
from petroineos_challenge.services.power_service import TradePosition

mock_trade_volumes = [
    TradePosition(trader_id=1, trade_date=date.today(), period=1, volume=100),
    TradePosition(trader_id=1, trade_date=date.today(), period=2, volume=100),
    TradePosition(trader_id=1, trade_date=date.today(), period=3, volume=50),
    TradePosition(trader_id=2, trade_date=date.today(), period=1, volume=120),
    TradePosition(trader_id=2, trade_date=date.today(), period=2, volume=60),
    TradePosition(trader_id=2, trade_date=date.today(), period=3, volume=-20),
    TradePosition(trader_id=1, trade_date=date.today(), period=4, volume=10),
]

mock_trades_return = MagicMock()
mock_trades_return.side_effect = [mock_trade_volumes]


def test_trade_periodic_aggregation():
    rpt_service = IntraDayReportService(date.today(), "./report_path")
    with patch("petroineos_challenge.services.power_service.PowerService.get_trades", mock_trades_return):
        actual = rpt_service.generate_intraday_report()
        for rec in actual:
            if rec["Local Time"] == "23:00":
                assert rec["Volume"] == 220
            elif rec["Local Time"] == "00:00":
                assert rec["Volume"] == 160
            elif rec["Local Time"] == "01:00":
                assert rec["Volume"] == 30
            elif rec["Local Time"] == "02:00":
                assert rec["Volume"] == 10
            else:
                assert rec["Volume"] == 0

