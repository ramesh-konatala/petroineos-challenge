import random
from datetime import date

from pydantic import BaseModel

from petroineos_challenge.utils.logger import get_logger

logger = get_logger(__name__)


class TradePosition(BaseModel):
    trader_id: int
    trade_date: date
    period: int
    volume: int


class PowerService:
    def __init__(self, trade_date: date):
        self.trade_date = trade_date

    def _create_mock_data(self):
        trades = []
        for i in range(1, 11):
            for j in range(1, 25):
                trade_position = TradePosition(trader_id=i, trade_date=self.trade_date, period=j, volume=random.randint(-20, 100))
                trades.append(trade_position)
        logger.info("PowerService Mock Trade Positions data created successfully")
        return trades

    def get_trades(self):
        data = self._create_mock_data()
        return data

