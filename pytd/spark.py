import logging
import os
import re
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import urlopen

TD_SPARK_BASE_URL = "https://s3.amazonaws.com/td-spark/"
logger = logging.getLogger(__name__)


def download_td_spark(spark_binary_version="2.11", version="latest", destination=None):
    td_spark_jar_name = "td-spark-assembly_{}-{}.jar".format(
        spark_binary_version, version
    )

    if destination is None:
        destination = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), td_spark_jar_name
        )

    download_url = urljoin(TD_SPARK_BASE_URL, td_spark_jar_name)
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


def fetch_td_spark_context(
    apikey, endpoint, td_spark_path, download_if_missing, spark_configs
):
    try:
        from pyspark.conf import SparkConf
        from pyspark.sql import SparkSession
        import td_pyspark
        from td_pyspark import TDSparkContextBuilder
    except ImportError:
        raise RuntimeError("td_pyspark is not installed")

    conf = (
        SparkConf()
        .setMaster("local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.execution.arrow.enabled", "true")
    )
    if isinstance(spark_configs, dict):
        for k, v in spark_configs.items():
            conf.set(k, v)
    builder = TDSparkContextBuilder(SparkSession.builder.config(conf=conf))

    builder.apikey(apikey)

    if td_spark_path is None:
        td_spark_path = TDSparkContextBuilder.default_jar_path()

    available = os.path.exists(td_spark_path)

    if not available and download_if_missing:
        download_td_spark(version=td_pyspark.__version__, destination=td_spark_path)
    elif not available:
        raise IOError("td-spark is not found and `download_if_missing` is False")

    builder.jars(td_spark_path)

    plazma_api = os.getenv("TD_PLAZMA_API")
    presto_api = os.getenv("TD_PRESTO_API")

    if plazma_api and presto_api:
        api_regex = re.compile(r"(?:https?://)?(api(?:-.+?)?)\.")
        builder.api_endpoint(api_regex.sub("\\1.", endpoint).strip("/"))
        builder.plazma_endpoint(plazma_api)
        builder.presto_endpoint(presto_api)

    site = "us"
    if ".co.jp" in endpoint:
        site = "jp"
    if "eu01" in endpoint:
        site = "eu01"
    builder.site(site)

    try:
        return builder.build()
    except Exception as e:
        raise RuntimeError("failed to connect to td-spark: " + str(e))
