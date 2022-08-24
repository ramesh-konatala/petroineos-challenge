# Power Traders Intra-day Report Service
This is a Python application to generate intra-day report to give power traders their day ahead power position

The repo contains code base to create the report using basic python modules along with Pyspark modules.
The main.py module have relevant placeholders to enable spark modules for running the code in Spark environment.

## Execution

### Command Line

```shell
>>> pipenv run python -m petroineos_challenge.main --report_date="2022-08-23" --report_path="report_location"
```

#### Command Line Arguments
````
Argument                Descriton                                           Example
========                =========                                           =======
report_date             date for which intra-day report to be exxcuted      YYYY-MM-DD Format
report_path             file location string where report to be saved       /home/intra-reports/
````

### Using Python Modules
Pure Python Modules
```python
>>> from datetime import date
>>> from petroineos_challenge.services.intraday_report import IntraDayReportService
>>> rpt_service = IntraDayReportService(date.today())
>>> rpt_service.create_report()
```
Pyspark Modules
```python
>>> from petroineos_challenge.services.spark_report_service import SparkReportService
>>> spark_rpt_service = SparkReportService(report_date, report_path)
>>> spark_rpt_service.create_report()
```

### Note
An attempt was made to use the provided dll as a wrapper service but somehow not working in my local MAC env, hence mocked python service for replicating PowerService functionality.
    
```python
import clr
from pathlib import PurePath
from datetime import datetime
BASE_PATH = PurePath(__file__).parent
DLL_PATH = BASE_PATH / "dlls" / "PowerService"
clr.AddReference(str(DLL_PATH))
from Services import PowerService
ps = PowerService(datetime.now())
```

