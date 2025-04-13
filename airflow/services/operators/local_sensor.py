from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import os

class LocalFileSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, filepath, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath

    def poke(self, context):
        return os.path.exists(self.filepath)