import os
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class LocalFileSensor(BaseSensorOperator):

    template_fields = ("filepath",)

    @apply_defaults
    def __init__(self, filepath: str, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath

    def poke(self, context):
        if os.path.exists(self.filepath):
            return True
        return AirflowException(f"File {self.filepath} not found")