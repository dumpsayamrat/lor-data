from dagster import repository, make_python_type_usable_as_dagster_type
from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from pyspark.sql import DataFrame
# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(python_type=DataFrame, dagster_type=DagsterPySparkDataFrame)

from pipelines.jobs.say_hello import say_hello_job
from pipelines.schedules.my_hourly_schedule import my_hourly_schedule
from pipelines.sensors.my_sensor import my_sensor
from pipelines.jobs.loading_lor_data import get_lor_win_rate_job


@repository
def pipelines():
  """
  The repository definition for this pipelines Dagster repository.

  For hints on building your Dagster repository, see our documentation overview on Repositories:
  https://docs.dagster.io/overview/repositories-workspaces/repositories
  """
  jobs = [say_hello_job, get_lor_win_rate_job]
  schedules = [my_hourly_schedule]
  sensors = [my_sensor]

  return jobs + schedules + sensors
