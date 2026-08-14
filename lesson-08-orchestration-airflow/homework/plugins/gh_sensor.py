"""
Custom Airflow Sensor: GHArchiveSensor.

Перевіряє доступність годинного файлу GitHub Archive через HTTP HEAD.
Якщо файл ще не з'явився — чекає. Коли доступний (HTTP 200) — пропускає flow далі.

Використовується як перша задача DAG github_archive_daily.
"""

import urllib.request
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class GHArchiveSensor(BaseSensorOperator):
    """
    Sensor, що чекає доступності годинного файлу GitHub Archive.

    URL формату: https://data.gharchive.org/{ds}-{hour:02d}.json.gz
    Перевірка — HTTP HEAD запит. Повертає True при HTTP 200.

    Параметри:
        hour (int): година файлу (0-23), за замовчуванням 14.
    """

    def __init__(self, hour: int = 14, **kwargs):
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context: Context) -> bool:
        """
        Виконує HTTP HEAD до GH Archive.

        Дату бере з context["ds"] (logical_date Airflow) — забезпечує ідемпотентність.
        """
        ds = context["ds"]  # формат YYYY-MM-DD, від Airflow logical_date
        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"
        self.log.info("Checking availability: %s", url)

        req = urllib.request.Request(
            url,
            method="HEAD",
            headers={"User-Agent": "airflow-gh-sensor/1.0"},
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    self.log.info("✅ File available: %s", url)
                    return True
                return False
        except Exception as e:
            self.log.info("⏳ Not yet available (%s), will retry...", e)
            return False
