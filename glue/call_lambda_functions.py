import sys
import json
import math
import pyspark.sql.functions as sqlf
import requests

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, array
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


SOCIO_ECONOMIL_LEVEL_URL = "URL?params={params}"
PRICE_URL = "URL?params={params}"
GET_CURRENCI_URL = "URL?params={params}"

def call_functions(row):
	start, end, radius = map(float, row)
	level = requests.get(SOCIO_ECONOMIL_LEVEL_URL.format(params="{start},{end},{radius}".format(start=start, end=end, radius=radius)))
	price_square_meter = requests.get(PRICE_URL.format(params=level))
	curenci = requests.get(GET_CURRENCI_URL.format("USD-MXN"))
	mxn_price = price_square_meter*curenci
	return mxn_price

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# getting the data
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = database,
    table_name = table_name,
    push_down_predicate=predicate,
	transformation_ctx = "datasource"
)

udf_call_functions = udf(lambda r: call_functions(r), FloatType())

df = datasource.toDF()

df.parallelize(map(lambda x: udf_call_functions(x)))