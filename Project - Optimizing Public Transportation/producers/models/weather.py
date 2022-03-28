"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from producers.models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            "chicago_weather", # TODO: Come up with a better topic name
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=3,
            num_replicas=3
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        """
        Producing a weather event using REST Proxy.
        :param month:
        :return:
        """
        self._set_weather(month)
        # Setting the appropriate headers
        # See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
        data = {
                "key_schema": Weather.key_schema,,
                "value_schema": Weather.value_schema,
                "records": [{"value":{'temperature': self.temp, 'status': self.status}}]
        }
        resp = requests.post(
                f"{Weather.rest_proxy_url}/topics/chicago.weather",
                data = json.dumps(data),
                headers = headers
        )

        try:
            resp.raise_for_status()
        except:
            logger.warning(
                    "Failed to send weather data to kafka, temp: %s, status: %s",
                    self.temp,
                    self.status.name
            )

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
