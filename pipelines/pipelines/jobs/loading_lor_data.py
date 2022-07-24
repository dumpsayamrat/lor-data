from dagster import graph, ResourceDefinition, IOManager, io_manager
from dagster_pyspark import pyspark_resource
from pipelines.ops.lor_scraping import get_lor_win_rate, write_to_file

class ParquetIOManager(IOManager):
  def _get_path(self, context):
    return "/".join(
        [context.resource_config["path_prefix"], context.run_id, context.step_key, context.name]
    )

  def handle_output(self, context, obj):
    obj.write.parquet(self._get_path(context))

  def load_input(self, context):
    spark = context.resources.pyspark.spark_session
    return spark.read.parquet(self._get_path(context.upstream_output))


@io_manager(required_resource_keys={"pyspark"}, config_schema={"path_prefix": str})
def parquet_io_manager():
    return ParquetIOManager()

@graph
def get_lor_win_rate_graph():
  write_to_file(get_lor_win_rate())

local_resource_defs = {
  "pyspark_step_launcher": ResourceDefinition.none_resource(),
  "pyspark": pyspark_resource.configured({"spark_conf": {"spark.default.parallelism": 1}}),
  "io_manager": parquet_io_manager.configured({"path_prefix": "."}),
}

get_lor_win_rate_job = get_lor_win_rate_graph.to_job(
  resource_defs=local_resource_defs
)
