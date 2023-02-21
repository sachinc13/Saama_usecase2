import os
from io import BytesIO
import sys
import pyspark
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
import shutil
from zipfile import *
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def unzip_file_fn():
    for file in my_bucket.objects.filter(Prefix=inbound_path):
        file1=file.key
        if file1.endswith(zip_file) is True:
            zip_obj = s3.Object(bucket_name=s3_bucket_name ,key=file1)
            buffer = BytesIO(zip_obj.get()["Body"].read())
            z = ZipFile(buffer)
            for filename in z.namelist():
                s3.meta.client.upload_fileobj(
                z.open(filename),
                Bucket=s3_bucket_name,
                Key=temp_path + filename,
                Config=None
                )


def move_in_landing():
    foldername_list = set()
    for file in my_bucket.objects.filter(Prefix=temp_path):
        file1=file.key
        file2=(file.key).split('/')[-1]
        if file1.endswith(gz_file) is True:
            copy_source = {
                'Bucket': s3_bucket_name,
                'Key': file.key
            }
            read_file = pd.read_csv("s3://"+s3_bucket_name+"/"+file1, compression='gzip', header=0, sep=',', quotechar='"')
            read_file_spark= spark.createDataFrame(read_file)
            
            read_file_spark = read_file_spark.withColumn("Open",col("Open").cast((DoubleType()))).withColumn("High",col("High").cast((DoubleType()))).withColumn("Low",col("Low").cast((DoubleType()))).withColumn("Close",col("Close").cast((DoubleType())))
            
            read_file = read_file_spark.toPandas()
            
            read_file.to_csv("s3://"+s3_bucket_name+"/"+landing_path+file2.split('/')[-1].split('.')[0].split('_')[0]+"/"+file2.split('/')[-1].split('.')[0].split('_')[1]+"/"+"sachin-UC2-"+file2.split('/')[-1].split('.')[0]+".csv", index = None,header=True,sep=",")
            

def create_crawler():
    foldername_list = set()
    print('Start to Create crawler!...')
    for file in my_bucket.objects.filter(Prefix=temp_path):
        file2=(file.key).split('/')[-1]
        foldername_list.add(file2.split('/')[-1].split('.')[0].split('_')[0])
    for x in foldername_list:
        if x !='':
            objects = list(my_bucket.objects.filter(Prefix=landing_path + x +'/'))
            objects.sort(key=lambda o: o.last_modified)
            CrawlerName = 'saama-gene-training-sachin-crawler-'+x+"-"+objects[-1].key.split("_")[-1].split(".")[0]
            path = "s3://"+s3_bucket_name+"/"+landing_path+x+"/"+objects[-1].key.split("_")[-1].split(".")[0]+"/"
            crawler_details=glue_client.list_crawlers()
            if CrawlerName not in crawler_details['CrawlerNames']:
                response = glue_client.create_crawler(
                    Name=CrawlerName,
                    Role='saama-gene-training-glue-service-role',
                    DatabaseName='saama-gene-training-data',
                    Targets={
                        'S3Targets': [
                            {
                                'Path':path,
                            }
                        ]
                    },
                    TablePrefix=f'saama-gene-training-sachin_u2_'
                )
                print(f"{CrawlerName}  - Crawler is created successfully....")
            run_crawler(CrawlerName)
            
def run_crawler(CrawlerName):
    response = glue_client.start_crawler(
        Name=CrawlerName
        )
    print("Successfully started crawler")
    
def move_old_files_arch():
    foldername_list = set()
    for file in my_bucket.objects.filter(Prefix=temp_path):
        file2=(file.key).split('/')[-1]
        foldername_list.add(file2.split('/')[-1].split('.')[0].split('_')[0])
    for x in foldername_list:
        if x !='':
            objects = list(my_bucket.objects.filter(Prefix=landing_path + x +'/'))
            objects.sort(key=lambda o: o.last_modified)
            for i in range(len(objects) - 1):
                source_filename = (objects[i].key).split('/')[-1]
                copy_source = {
                    'Bucket': s3_bucket_name,
                    'Key': objects[i].key
                }
                target_filename_archive = "{}{}".format(temp_path,source_filename)
                s3.meta.client.copy(copy_source, s3_bucket_name, target_filename_archive)
                s3.Object(s3_bucket_name, objects[i].key).delete() 


if __name__ == "__main__":
    athena_client=boto3.client('athena',region_name="ap-south-1")
    s3_client=boto3.client('s3')
    s3=boto3.resource('s3')
    glue_client=boto3.client('glue',region_name="ap-south-1")
    s3_bucket_name='saama-gene-training-data-bucket'
    my_bucket=s3.Bucket(s3_bucket_name)
    zip_file='.zip'
    gz_file='.gz'
    inbound_path = 'sachinc/sachin2/Inbound/'
    temp_path = 'sachinc/sachin2/Temp/'
    landing_path = 'sachinc/sachin2/Landing/'

    #unzip_file_fn
    move_in_landing()
    # create_crawler()
    # move_old_files_arch()







