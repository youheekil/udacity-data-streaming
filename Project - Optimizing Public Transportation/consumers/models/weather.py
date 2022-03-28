"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        if message.topic() == 'chicago_weather':
            try:
                weather_json_data = json.loads(message.value())
                self.temperature = weather_json['temperature']
                self.status = weather_json['status']
            except:
                logger.debug(
                        "unable to handle incoming weather data"
                )
        else:
            logger.debug(
                    "unable to find handler for message from topic %s", message.topic()
            ) # TODO: NOT SURE IF message.topic() or message.topic

