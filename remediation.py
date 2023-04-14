from pyspark.sql.types import *
#from pyspark import RDD, since
from pyspark.sql import SparkSession
#from pyspark.sql.column import _to_seq
#from pyspark.sql.dataframe import *
from pyspark.sql.functions import udf,lit
import re
#from pyspark import storagelevel
spark = SparkSession.builder.appName("Remediation").getOrCreate()
#logger = spark._jvm.org.apache.log4j
#logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
#logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)

def remediate_violations(input_text,type_violation):
    if type_violation == "ein":
        output_val = (re.sub('(0[1-6]|1[0-6]|2[0-7]|3[0-9]|4[0-8]|5[0-9]|6[0-8]|7[1-7]|8[0-8]|9[8-9])(-)(\\d{3})(\\d{4})( )(\\d{3}|[a-zA-Z])','XX\\2XXX\\4\\5\\6', input_text))
        return output_val
    elif type_violation == "csin":
        output_val = (re.sub('([0-9]{3})([ ])([0-9]{3})([ ])([0-9]{3})','XXX\\2XXX\\4\\5', input_text))
        return output_val
    elif type_violation == "ssn":
        output_val = (re.sub('([0-8][0-9]{2})([ ])([0-9]{2})([ ])([0-9]{4})','XXX\\2XX\\4\\5', input_text))
        return output_val
    elif type_violation == "php_ssn":
        output_val = (re.sub('([0-9]{2})([ ])([0-9]{4})([0-9]{3})([ ])([0-9])','XX\\2XXXX\\4\\5\\6', input_text))
        return output_val
    elif type_violation == "itin":
        output_val = (re.sub('(9[0-9]{2})([ ])([7][0-9]|[8][0-8]|[9][0-2]|[9][4-9])([ ])([0-9]{4})','XXX\\2XX\\4\\5', input_text))
        return output_val
    else:
        return None


udf_remediate_violation = udf(remediate_violations,StringType())


file_path = "C://python/Innovation/remed_input.csv"

src_df = spark.read.csv(file_path,sep="|")
src_df.show(10,False)
trg_df = src_df.withColumn("_c0",udf_remediate_violation("_c0",lit("csin"))).cache()\
              .withColumn("_c0",udf_remediate_violation("_c0",lit("ssn"))).cache()


trg_df.show(10,False)
