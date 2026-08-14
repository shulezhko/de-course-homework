"""Custom sensor — чекає появи файлу на GitHub Archive (HTTP HEAD)."""

import urllib.request
from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    """Чекає, поки файл за дату ds і годину hour з'явиться на gharchive.org."""

    def __init__(self, hour: int = 14, **kwargs):
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"
        self.log.info("HEAD %s", url)

        req = urllib.request.Request(url, method="HEAD",
                                     headers={"User-Agent": "gh-sensor/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except Exception:
            return False
