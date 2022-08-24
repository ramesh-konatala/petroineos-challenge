import csv
from datetime import date, timedelta
from pathlib import PurePath
from typing import Dict, List, Optional

import pandas as pd


def dict_to_csv(dict_list: List[dict], file_path: PurePath, headers: Optional[List[str]] = None,
                dialect="excel") -> None:
    if not headers:
        headers = dict_list[0].keys()
    with open(file_path, 'w') as f:
        writer = csv.DictWriter(f, lineterminator="\n", dialect=dialect, fieldnames=headers)
        writer.writeheader()
        writer.writerows(dict_list)


def create_period_map() -> Dict[int, str]:
    period_map = {}
    data_dates = pd.date_range(start=date.today(), end=date.today() + timedelta(days=1), freq='H')
    df = pd.DataFrame(data_dates, columns=["timestamp"])
    period = 1
    for index, ts in df["timestamp"].items():
        hour = ts.hour - 1
        if hour == -1:
            hour = 23
        period_map[period] = str(hour).rjust(2, '0') + ":00"
        if period >= 24:
            break
        period += 1

    return period_map
