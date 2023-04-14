import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME","file","bucket"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
file_name=args['file']
bucket_name=args['bucket']
input_file_path="s3://{}/{}".format(bucket_name,file_name)
#print(input_file_path)
dataload = file_name.split('/')[-2]

#dyf = glueContext.create_dynamic_frame.from_catalog(database='remediation_database', table_name='customer_demo_csv')
#dyf.printSchema()

dyf = glueContext.create_dynamic_frame_from_options(connection_type= 's3',
                                                               connection_options={"paths": [input_file_path]},
                                                               format='csv',format_options = {"withHeader": True})
dyf.printSchema()
dyf.show()
spark_source_Df  = dyf.toDF()
#sparkDf .show()
#Read the config file from S3 and convert to sparkDF
s3_path = 's3://aws-glue-remediation-demo/data/config_file/'
config_file_df = glueContext.create_dynamic_frame_from_options(connection_type= 's3',
                                                               connection_options={"paths": [s3_path]},
                                                               format='json')
spark_config_file_df  = config_file_df.toDF()

data_collect = spark_config_file_df.collect()


#Identify the sensitive data and mask it

from pyspark.sql.types import *
#from pyspark import RDD, since
from pyspark.sql import SparkSession
#from pyspark.sql.column import _to_seq
#from pyspark.sql.dataframe import *
from pyspark.sql.functions import udf,lit
import re

def remediate_violations(input_text,type_violation):
    if type_violation == "ein":
        output_val = (re.sub('(0[1-6]|1[0-6]|2[0-7]|3[0-9]|4[0-8]|5[0-9]|6[0-8]|7[1-7]|8[0-8]|9[8-9])(-)(\\d{3})(\\d{4})( )(\\d{3}|[a-zA-Z])','XX\\2XXX\\4\\5\\6', input_text))
        #output_val = (re.sub('(0[1-6]|1[0-6]|2[0-7]|3[0-9]|4[0-8]|5[0-9]|6[0-8]|7[1-7]|8[0-8]|9[8-9])( )(\\d{3})(\\d{4})( )(\\d{3}|[a-zA-Z])','XX\\2XXX\\4\\5\\6', input_text))        
        return output_val
    elif type_violation == "csin":
        output_val = (re.sub('([0-9]{3})([ ])([0-9]{3})([ ])([0-9]{3})','XXX\\2XXX\\4\\5', input_text))
        output_val = (re.sub('([0-9]{3})([-])([0-9]{3})([-])([0-9]{3})','XXX\\2XXX\\4\\5', input_text))        
        return output_val
    elif type_violation == "ssn":
        output_val = (re.sub('([0-8][0-9]{2})([ ])([0-9]{2})([ ])([0-9]{4})','XXX\\2XX\\4\\5', input_text))
        output_val = (re.sub('([0-8][0-9]{2})([-])([0-9]{2})([-])([0-9]{4})','XXX\\2XX\\4\\5', input_text))        
        return output_val
    elif type_violation == "php_ssn":
        output_val = (re.sub('([0-9]{2})([ ])([0-9]{4})([0-9]{3})([ ])([0-9])','XX\\2XXXX\\4\\5\\6', input_text))
        output_val = (re.sub('([0-9]{2})([-])([0-9]{4})([0-9]{3})([-])([0-9])','XX\\2XXXX\\4\\5\\6', input_text))        
        return output_val
    elif type_violation == "itin":
        output_val = (re.sub('(9[0-9]{2})([ ])([7][0-9]|[8][0-8]|[9][0-2]|[9][4-9])([ ])([0-9]{4})','XXX\\2XX\\4\\5', input_text))
        #output_val = (re.sub('(9[0-9]{2})([-])([7][0-9]|[8][0-8]|[9][0-2]|[9][4-9])([-])([0-9]{4})','XXX\\2XX\\4\\5', input_text))        
        return output_val
    else:
        return None
    
udf_remediate_violation = udf(remediate_violations,StringType())
target_path = data_collect[0]['target_path']
target_path = target_path+dataload
for row in data_collect:
    trg_all_mask_df = spark_source_Df
    for i in range(0,len(row['colomn_param'])):
        i_sensitive_type = row['colomn_param'][i]['sensitive_type']
        i_column_name    = row['colomn_param'][i]['column_name']
        trg_all_mask_df = trg_all_mask_df.withColumn(i_column_name,udf_remediate_violation(i_column_name,lit(i_sensitive_type))).cache()

trg_all_mask_df.show(10,False)

#convert from spark Dataframe to Glue Dynamic DataFrame

from awsglue.dynamicframe import DynamicFrame

#Convert from Spark Data Frame to Glue Dynamic Frame
masked_df = DynamicFrame.fromDF(trg_all_mask_df, glueContext, "convert")

#Show converted Glue Dynamic Frame
#masked_df.show()
# write down the data in converted Dynamic Frame to S3 location. 
glueContext.write_dynamic_frame.from_options(
                            frame = masked_df,
                            connection_type="s3", 
                            connection_options = {"path": target_path}, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
job.commit()
