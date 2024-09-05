import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from joblib import variables as V
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# read data from S3
s3_file_topcv = f's3://{V.DATA_LANDING_BUCKET_NAME}/raw_zone/job_it_topcv.json'
topcv = spark.read.option('multiline', 'true').json(s3_file_topcv)

# logic transforms
# Remove all HTML tags from a string
html_pattern =r"<(?:\"[^\"]*\"['\"]*|'[^']*'['\"]*|[^'\">])+>"

topcv = topcv.withColumn('description',F.regexp_extract(F.col("description"),html_pattern, 0)) \
        .withColumn("requirement",F.regexp_extract(F.col("requirement"),html_pattern, 0))\
        .withColumn("benefit",F.regexp_extract(F.col("benefit"),html_pattern, 0))\
            .select('title',
                    'salary',
                    'location',
                    'exp',
                    'description',
                    'requirement',
                    'benefit',
                    'working_location',
                    'working_time',
                    'company'
                    )
    
# Get District in company address string
hanoi_districts = [  
    "Ba Đình",  
    "Hoàn Kiếm",  
    "Tây Hồ",  
    "Cầu Giấy",  
    "Đống Đa",  
    "Hai Bà Trưng",  
    "Thanh Xuân",  
    "Hoàng Mai",  
    "Long Biên",  
    "Hà Đông",  
    "Bắc Từ Liêm",  
    "Nam Từ Liêm"  
]
def find_keywords(text):
    matches = [district for district in hanoi_districts if district in text]
    return ', '.join(matches) if matches else None
find_keywords_udf = F.udf(find_keywords, StringType())

df_district = topcv.withColumn("district", find_keywords_udf(F.col("working_location")))\
                    .select('title',
                    'salary',
                    'location',
                    'exp',
                    'description',
                    'requirement',
                    'benefit',
                    'working_location',
                    'working_time',
                    'company',
                    'role',
                    'district'
                    )
#write to s3
s3_saving_path = f's3://{V.DATA_LANDING_BUCKET_NAME}/golden_zone/job_detail'

df_district.coalesce(1).write.mode('overwrite').parquet(s3_saving_path)
job.commit()