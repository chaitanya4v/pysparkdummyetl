# Databricks notebook source
# import all the required functions

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import re
import uuid

# COMMAND ----------

# Defining Generic functions

def read_parquet_data(path):
  ip_df = spark.read.parquet(path)
  return ip_df

# COMMAND ----------

########## Environment setup ###########
env = 'Development'
# env = 'QA'


# Defining the file paths
cdp_dw_theme_path = '/Path_of_parquet_blob'+env+'/Path_of_parquet_blob/cdp3_themeBase.parquet'
cdp_dw_StringMapBase_path = '/Path_of_parquet_blob'+env+'/Path_of_parquet_blob/StringMapBase.parquet'
cdp_dw_Entity_path = '/Path_of_parquet_blob'+env+'/Path_of_parquet_blob/Entity.parquet'

df_theme_delta_path = "/Path_of_parquet_blob"+env+"/Silver/CRM/TB_CRM_THEME"
df_theme_delta_PostDataset_path = '/Path_of_parquet_blob'+env+'/Silver/PostDataset/SILVER_DIM_THEME'

# COMMAND ----------

# Reading the parquet files
cdp_dw_theme_df = read_parquet_data(cdp_dw_theme_path)

cdp_dw_StringMapBase_df = read_parquet_data(cdp_dw_StringMapBase_path)
cdp_dw_StringMapBase_df = cdp_dw_StringMapBase_df.drop(cdp_dw_StringMapBase_df.OrganizationId)

cdp_dw_Entity_df = read_parquet_data(cdp_dw_Entity_path)

# Drop duplicates
cdp_dw_theme_df=cdp_dw_theme_df.dropDuplicates()
cdp_dw_StringMapBase_df=cdp_dw_StringMapBase_df.dropDuplicates()
cdp_dw_Entity_df=cdp_dw_Entity_df.dropDuplicates()


# COMMAND ----------

# Join condition for "CRM_STATUS_REASON" column

cdp_dw_Entity_df = cdp_dw_Entity_df.filter("OverwriteTime = '1900-01-01 00:00:00.000' AND ComponentState = 0 and name = 'cdp3_theme'").select("ObjectTypeCode")

ObjTypeCode_Val=cdp_dw_Entity_df.collect()[0][0]

cdp_dw_theme_df_final=cdp_dw_theme_df.join(cdp_dw_StringMapBase_df, (cdp_dw_theme_df.statuscode == cdp_dw_StringMapBase_df.AttributeValue) & (cdp_dw_StringMapBase_df.AttributeName == "statuscode") & (cdp_dw_StringMapBase_df.ObjectTypeCode == ObjTypeCode_Val),"left")

# COMMAND ----------

# Generate UUID for new column
# Create UDF function to create surrogate key

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# schema = StructType([StructField("CRM_THEME_ID",StringType(),True),StructField("THEME_NAME",StringType(),True),StructField("CRM_ORGANISATION_ID",StringType(),True),StructField("CRM_STATUS",IntegerType(), True),StructField("CRM_STATUS_REASON", StringType(), True) ])

# data_theme= [("-1","SER","14d9e776-c11b-e811-9100-0050569ce4f2",0,"Active") ]
# df_theme_data_extra = spark.createDataFrame(data=data_theme,schema=schema)
# df_theme_data_extra= df_theme_data_extra.withColumn("CRM_CREATED_ON",current_timestamp()).withColumn("CRM_MODIFIED_ON",current_timestamp())
# display(df_theme_data_extra)

# COMMAND ----------

# Create a dataframe with required columns

UTCTIME = getArgument('UTCTIME')
#UTCTIME= '2021-12-07T13:10:23.7412073Z'
cdp_dw_theme_df=cdp_dw_theme_df_final.select(
  cdp_dw_theme_df_final.cdp3_themeId.alias("CRM_THEME_ID"),
  cdp_dw_theme_df_final.cdp3_name.alias("THEME_NAME"),
  cdp_dw_theme_df_final.OrganizationId.alias("CRM_ORGANISATION_ID"),
  cdp_dw_theme_df_final.statecode.alias("CRM_STATUS"),
  cdp_dw_theme_df_final.Value.alias("CRM_STATUS_REASON"),
  cdp_dw_theme_df_final.CreatedOn.alias("CRM_CREATED_ON"),
  cdp_dw_theme_df_final.ModifiedOn.alias("CRM_MODIFIED_ON"),
)
#cdp_dw_theme_df=cdp_dw_theme_df.union(df_theme_data_extra)

cdp_dw_theme_df=cdp_dw_theme_df.withColumn("DW_CREATE_DATE",lit(UTCTIME)).withColumn("DW_MODIFIED_DATE",lit(UTCTIME)).withColumn("THEME_ID",uuidUdf())

cdp_dw_theme_df=cdp_dw_theme_df.withColumn("DW_CREATE_DATE",cdp_dw_theme_df.DW_CREATE_DATE.cast('timestamp')).withColumn("DW_MODIFIED_DATE",cdp_dw_theme_df.DW_MODIFIED_DATE.cast('timestamp'))

cdp_dw_theme_df = cdp_dw_theme_df.withColumn(
  "CRM_STATUS", when((cdp_dw_theme_df.CRM_STATUS == 0),lit(1)).otherwise(lit(0)))

cdp_dw_theme_df.createOrReplaceTempView("vw_theme_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver_dev.SILVER_TB_CRM_THEME AS target
# MAGIC 
# MAGIC USING vw_theme_data AS source
# MAGIC ON target.CRM_THEME_ID=source.CRM_THEME_ID
# MAGIC 
# MAGIC WHEN MATCHED 
# MAGIC AND (target.THEME_NAME != source.THEME_NAME 
# MAGIC OR target.CRM_ORGANISATION_ID != source.CRM_ORGANISATION_ID 
# MAGIC OR target.CRM_STATUS != source.CRM_STATUS 
# MAGIC OR target.CRM_STATUS_REASON != source.CRM_STATUS_REASON) THEN
# MAGIC 
# MAGIC UPDATE SET target.THEME_NAME = source.THEME_NAME, target.DW_MODIFIED_DATE = source.DW_MODIFIED_DATE,
# MAGIC target.CRM_STATUS = source.CRM_STATUS,target.CRM_STATUS_REASON = source.CRM_STATUS_REASON
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (CRM_THEME_ID,THEME_NAME,CRM_ORGANISATION_ID,CRM_STATUS,CRM_STATUS_REASON,CRM_CREATED_ON,CRM_MODIFIED_ON,DW_CREATE_DATE,DW_MODIFIED_DATE,THEME_ID) VALUES(CRM_THEME_ID,THEME_NAME,CRM_ORGANISATION_ID,CRM_STATUS,CRM_STATUS_REASON,CRM_CREATED_ON,CRM_MODIFIED_ON,DW_CREATE_DATE,DW_MODIFIED_DATE,THEME_ID)

# COMMAND ----------

#Read from delta and write with incremented data

df_theme_delta = spark.read.format('delta').load(df_theme_delta_path)

df_theme_delta.write.mode("overwrite").format("parquet").save(df_theme_delta_PostDataset_path)

# COMMAND ----------

# #UUID generation

# df_theme_delta = spark.read.format('delta').load("/mnt/root/CDP/Development/Silver/CRM/TB_CRM_THEME")
# df_theme_uuid= df_theme_delta.select(df_theme_delta.THEME_ID.alias('UUID'))
# df_theme_uuid.write.mode("overwrite").format("parquet").save('/mnt/root/CDP/Development/Config/SilverUUID/SILVER_DIM_THEME')

# #df_theme_delta.write.mode("overwrite").format("parquet").save('/mnt/root/CDP/Development/Silver/PostDataset/SILVER_DIM_THEME')


# COMMAND ----------

# # Read from Delta & Write to parquet

# df_theme_delta = spark.read.format('delta').load("/mnt/root/CDP/Development/Silver/MasterData/TB_CRM_THEME")
# df_waterMark= spark.read.format('parquet').load('/mnt/root/CDP/Development/Config/Silver_waterMark/SILVER_DIM_THEME_WATERMARK')

# WaterMark=df_waterMark.collect()[0][0]
# df_theme_delta_incremented = df_theme_delta.filter(df_theme_delta.DW_MODIFIED_DATE >= WaterMark)

# df_theme_delta_incremented.write.mode("overwrite").format("parquet").save('/mnt/root/CDP/Development/Silver/PostDataset/SILVER_DIM_THEME')

# WaterMark_theme_max_modified_dt = df_theme_delta.select(max(df_theme_delta.DW_MODIFIED_DATE).alias('MaxModifiedOn'))
# WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/mnt/root/CDP/Development/Config/Silver_waterMark/SILVER_DIM_THEME_WATERMARK')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- CREATE TABLE silver_dev.SILVER_TB_CRM_THEME
# MAGIC -- (
# MAGIC -- CRM_THEME_ID string,
# MAGIC -- THEME_NAME string,
# MAGIC -- CRM_ORGANISATION_ID string,
# MAGIC -- CRM_STATUS int,
# MAGIC -- CRM_STATUS_REASON string,
# MAGIC -- CRM_CREATED_ON timestamp,
# MAGIC -- CRM_MODIFIED_ON timestamp,
# MAGIC -- DW_CREATE_DATE timestamp,
# MAGIC -- DW_MODIFIED_DATE timestamp,
# MAGIC -- THEME_ID string
# MAGIC -- )
# MAGIC -- using delta
# MAGIC -- location '/mnt/root/CDP/Development/Silver/CRM/TB_CRM_THEME'
# MAGIC 
# MAGIC 
# MAGIC -- CREATE TABLE silver_qa.SILVER_TB_CRM_THEME
# MAGIC -- (
# MAGIC -- CRM_THEME_ID string,
# MAGIC -- THEME_NAME string,
# MAGIC -- CRM_ORGANISATION_ID string,
# MAGIC -- CRM_STATUS int,
# MAGIC -- CRM_STATUS_REASON string,
# MAGIC -- CRM_CREATED_ON timestamp,
# MAGIC -- CRM_MODIFIED_ON timestamp,
# MAGIC -- DW_CREATE_DATE timestamp,
# MAGIC -- DW_MODIFIED_DATE timestamp,
# MAGIC -- THEME_ID string
# MAGIC -- )
# MAGIC -- using delta
# MAGIC -- location '/mnt/root/CDP/QA/Silver/CRM/TB_CRM_THEME'

# COMMAND ----------

