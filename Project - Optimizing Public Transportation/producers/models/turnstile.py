# producers.models.turnstile.py

"""
Creates a turnstile data producer
- turnstile indeicates passenger enters the station

Author: Youhee
Date: March, 2022
"""

import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        super().__init__(
            topic_name="turnstile_station", # TODO: DOUBLE CHECK
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    # TODO: TIME STAMP CHECK !
    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""

        # num_entries function returns maximum number of the calculation of approximation of number of
        # entries for the simulation step plus randomness in the data
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        # TODO: DOUBLE CHECK
        for _ in range(num_entries):
            self.producer.produce(
                    topic=self.topic_name,
                    key_schema=self.key_schema,
                    key={"timestamp": self.time_millis()},
                    value_schema=self.value_schema,
                    value={
                            "station_id"  : self.station.station_id,
                            "station_name": self.station.name,
                            "line"        : self.station.color.name
                    },
            )

