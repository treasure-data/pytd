import logging
import os
import re
from urllib.error import HTTPError
from urllib.request import urlopen

TD_SPARK_BASE_URL = "https://s3.amazonaws.com/td-spark/{}"
TD_SPARK_JAR_NAME = "td-spark-assembly_2.11-19.7.0.jar"
TD_SPARK_DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), TD_SPARK_JAR_NAME
)
logger = logging.getLogger(__name__)


def download_td_spark(download_url=None, destination=None):
    if download_url is None:
        download_url = TD_SPARK_BASE_URL.format(TD_SPARK_JAR_NAME)

    if destination is None:
        destination = TD_SPARK_DEFAULT_PATH

    try:
        response = urlopen(download_url)
    except HTTPError:
        raise RuntimeError("failed to access to the download URL: " + download_url)

    logger.info("Downloading td-spark...")
    try:
        with open(destination, "w+b") as f:
            f.write(response.read())
    except Exception:
        os.remove(destination)
        raise
    logger.info("Completed to download")

    response.close()


def fetch_td_spark(apikey, endpoint, td_spark_path, download_if_missing, spark_configs):
    try:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession
    except ImportError:
        raise RuntimeError("PySpark is not installed")

    conf = (
        SparkConf()
        .setMaster("local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.execution.arrow.enabled", "true")
    )
    conf.set("spark.td.apikey", apikey)

    if td_spark_path is None:
        td_spark_path = TD_SPARK_DEFAULT_PATH

    available = os.path.exists(td_spark_path)

    if not available and download_if_missing:
        download_td_spark(destination=td_spark_path)
    elif not available:
        raise IOError("td-spark is not found and `download_if_missing` is False")

    conf.set("spark.jars", td_spark_path)

    plazma_api = os.getenv("TD_PLAZMA_API")
    presto_api = os.getenv("TD_PRESTO_API")

    if plazma_api and presto_api:
        api_regex = re.compile(r"(?:https?://)?(api(?:-.+?)?)\.")
        conf.set("spark.td.api.host", api_regex.sub("\\1.", endpoint).strip("/"))
        conf.set("spark.td.plazma_api.host", plazma_api)
        conf.set("spark.td.presto_api.host", presto_api)

    site = "us"
    if ".co.jp" in endpoint:
        site = "jp"
    if "eu01" in endpoint:
        site = "eu01"
    conf.set("spark.td.site", site)

    if isinstance(spark_configs, dict):
        for k, v in spark_configs.items():
            conf.set(k, v)

    try:
        return SparkSession.builder.config(conf=conf).getOrCreate()
    except Exception as e:
        raise RuntimeError("failed to connect to td-spark: " + str(e))
