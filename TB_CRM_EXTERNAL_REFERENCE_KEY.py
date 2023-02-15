# Databricks notebook source
# import all the required functions

from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import uuid

# COMMAND ----------

# Defining Generic functions

def read_parquet_data(path):
  ip_df = spark.read.parquet(path)
  return ip_df

# COMMAND ----------

# Defining the file paths
cdp_dw_externalreferencekey_path = '/Path_of_parquet_blob/Bronze/883030-CRMDB/prod_crm_MSCRM/Merged/cdp3_externalreferencekeyBase.parquet'
cdp_dw_StringMapBase_path = '/Path_of_parquet_blob/Bronze/883030-CRMDB/prod_crm_MSCRM/Merged/StringMapBase.parquet'
cdp_dw_Entity_path = '/Path_of_parquet_blob/Bronze/883030-CRMDB/prod_crm_MSCRM/Merged/Entity.parquet'


# Reading the parquet files
cdp_dw_externalreferencekey_df = read_parquet_data(cdp_dw_externalreferencekey_path)

cdp_dw_StringMapBase_df = read_parquet_data(cdp_dw_StringMapBase_path)
cdp_dw_StringMapBase_df = cdp_dw_StringMapBase_df.drop(cdp_dw_StringMapBase_df.OrganizationId)

cdp_dw_Entity_df = read_parquet_data(cdp_dw_Entity_path)

# Drop duplicates
cdp_dw_externalreferencekey_df_initial=cdp_dw_externalreferencekey_df.dropDuplicates()
cdp_dw_StringMapBase_df=cdp_dw_StringMapBase_df.dropDuplicates()
cdp_dw_Entity_df=cdp_dw_Entity_df.dropDuplicates()
# cdp_dw_Entity_df=cdp_dw_Entity_df.filter("Name = 'cdp3_externalreferencekeyBase'") 
display(cdp_dw_externalreferencekey_df_initial)

# cdp_dw_Entity_df = cdp_dw_Entity_df["Name"]




# COMMAND ----------

# cdp_dw_Entity_df.select('Name').distinct().collect()

# COMMAND ----------

# Join condition for "CRM_STATUS_REASON" column
# "LEFT JOIN [dbo].[StringMap] String on 
# BADGEPHASE.statuscode = String.AttributeValue and 
# String.AttributeName = 'statuscode' and 
# String.ObjectTypeCode = (SELECT [ObjectTypeCode] FROM [dbo].[EntityView] where name = 'cdp3_badge')"


cdp_dw_Entity_df_filter = cdp_dw_Entity_df.filter("Name = 'cdp3_externalreferencekey'").select("ObjectTypeCode")
ObjTypeCode_Val=cdp_dw_Entity_df_filter.collect()[0][0] 

cdp_dw_externalreferencekey_df_initial_join = cdp_dw_externalreferencekey_df_initial.join(
  cdp_dw_StringMapBase_df, 
  (cdp_dw_externalreferencekey_df_initial.statuscode == cdp_dw_StringMapBase_df.AttributeValue
  ) & (
    cdp_dw_StringMapBase_df.AttributeName == "statuscode"
  ) & (
    cdp_dw_StringMapBase_df.ObjectTypeCode == ObjTypeCode_Val
  ),"left")


# COMMAND ----------

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

EXTERNAL_REFERENCE_KEY_df_final= cdp_dw_externalreferencekey_df_initial_join.select(
  cdp_dw_externalreferencekey_df_initial_join.cdp3_externalreferencekeyId.alias("CRM_EXTERNAL_REF_KEY_ID"),
  cdp_dw_externalreferencekey_df_initial_join.cdp3_name.alias("EXTERNAL_REFERENCE_KEY_NAME"),
  cdp_dw_externalreferencekey_df_initial_join.cdp3_description.alias("EXTERNAL_REFERENCE_KEY_DESCRIPTION"),
  cdp_dw_externalreferencekey_df_initial_join.statecode.alias("CRM_STATUS_CODE"),
  cdp_dw_externalreferencekey_df_initial_join.Value.alias("CRM_STATUS_REASON"),
  cdp_dw_externalreferencekey_df_initial_join.CreatedOn.alias("CRM_CREATED_ON"),
  cdp_dw_externalreferencekey_df_initial_join.ModifiedOn.alias("CRM_MODIFIED_ON"),

).withColumn("DW_CREATE_DATE",current_timestamp()).withColumn("DW_MODIFIED_DATE",current_timestamp()).withColumn("EXTERNAL_REFERENCE_KEY_ID",uuidUdf())

EXTERNAL_REFERENCE_KEY_df_final = EXTERNAL_REFERENCE_KEY_df_final.withColumn("CRM_STATUS_CODE", when((EXTERNAL_REFERENCE_KEY_df_final.CRM_STATUS_CODE == 0),lit(1)).otherwise(lit(0)))

EXTERNAL_REFERENCE_KEY_df_final.createOrReplaceTempView("vw_extr_ref_key_df")

# display(vw_extr_ref_key_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO master_data_dev.SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY AS target
# MAGIC 
# MAGIC USING vw_extr_ref_key_df AS source
# MAGIC ON target.CRM_EXTERNAL_REF_KEY_ID=source.CRM_EXTERNAL_REF_KEY_ID
# MAGIC 
# MAGIC WHEN MATCHED 
# MAGIC AND target.EXTERNAL_REFERENCE_KEY_NAME != source.EXTERNAL_REFERENCE_KEY_NAME 
# MAGIC AND target.EXTERNAL_REFERENCE_KEY_DESCRIPTION != source.EXTERNAL_REFERENCE_KEY_DESCRIPTION THEN
# MAGIC 
# MAGIC UPDATE SET 
# MAGIC 
# MAGIC target.EXTERNAL_REFERENCE_KEY_NAME = source.EXTERNAL_REFERENCE_KEY_NAME,
# MAGIC target.EXTERNAL_REFERENCE_KEY_DESCRIPTION = source.EXTERNAL_REFERENCE_KEY_DESCRIPTION,
# MAGIC target.CRM_STATUS_CODE = source.CRM_STATUS_CODE,
# MAGIC target.CRM_STATUS_REASON = source.CRM_STATUS_REASON,
# MAGIC target.DW_MODIFIED_DATE = current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC (EXTERNAL_REFERENCE_KEY_ID  ,CRM_EXTERNAL_REF_KEY_ID ,EXTERNAL_REFERENCE_KEY_NAME ,EXTERNAL_REFERENCE_KEY_DESCRIPTION ,CRM_STATUS_CODE , CRM_STATUS_REASON ,CRM_CREATED_ON ,CRM_MODIFIED_ON ,DW_CREATE_DATE ,DW_MODIFIED_DATE ) 
# MAGIC VALUES
# MAGIC (EXTERNAL_REFERENCE_KEY_ID  ,CRM_EXTERNAL_REF_KEY_ID ,EXTERNAL_REFERENCE_KEY_NAME ,EXTERNAL_REFERENCE_KEY_DESCRIPTION ,CRM_STATUS_CODE , CRM_STATUS_REASON ,CRM_CREATED_ON ,CRM_MODIFIED_ON ,DW_CREATE_DATE ,DW_MODIFIED_DATE )

# COMMAND ----------

# Read from Delta & Write to parquet

df_extr_ref_key_delta = spark.read.format('delta').load("/Path_of_parquet_blob/Silver/MasterData/TB_CRM_EXTERNAL_REFERENCE_KEY")

# Adding the watermark

df_waterMark= spark.read.format('parquet').load('/Path_of_parquet_blob/Config/Silver_waterMark/SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY_WATERMARK')

WaterMark=df_waterMark.collect()[0][0]

df_extr_ref_key_delta_incremented = df_extr_ref_key_delta.filter(df_extr_ref_key_delta.DW_MODIFIED_DATE >= WaterMark)

df_extr_ref_key_delta_incremented.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/Silver/PostDataset/SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY')

WaterMark_theme_max_modified_dt = df_extr_ref_key_delta.select(max(df_extr_ref_key_delta.DW_MODIFIED_DATE).alias('MaxModifiedOn'))

WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/Config/Silver_waterMark/SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY')

# COMMAND ----------

# df_acc_authority_delta_read = spark.read.format('delta').load("/Path_of_parquet_blob/Silver/MasterData/TB_CRM_EXTERNAL_REFERENCE_KEY")
# WaterMark_theme_max_modified_dt = df_acc_authority_delta_read.select(max(df_acc_authority_delta_read.DW_MODIFIED_DATE).alias('MaxModifiedOn'))
# WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/Config/Silver_waterMark/SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY_WATERMARK')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from master_data_dev.SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY
# MAGIC 
# MAGIC -- select * from vw_badge_df
# MAGIC 
# MAGIC -- SELECT COUNT(*)  FROM master_data_dev.SILVER_TB_CRM_BADGE
# MAGIC 
# MAGIC -- delete from master_data_dev.SILVER_TB_CRM_BADGE
# MAGIC 
# MAGIC  --TRUNCATE TABLE master_data_dev.SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY
# MAGIC 
# MAGIC --  CREATE TABLE master_data_dev.SILVER_TB_CRM_EXTERNAL_REFERENCE_KEY
# MAGIC -- (
# MAGIC -- EXTERNAL_REFERENCE_KEY_ID  String,
# MAGIC -- CRM_EXTERNAL_REF_KEY_ID String,
# MAGIC -- EXTERNAL_REFERENCE_KEY_NAME String,
# MAGIC -- EXTERNAL_REFERENCE_KEY_DESCRIPTION String,
# MAGIC -- CRM_STATUS_CODE INT,
# MAGIC -- CRM_STATUS_REASON String,
# MAGIC -- CRM_CREATED_ON timestamp,
# MAGIC -- CRM_MODIFIED_ON timestamp,
# MAGIC -- DW_CREATE_DATE timestamp,
# MAGIC -- DW_MODIFIED_DATE timestamp
# MAGIC 
# MAGIC --  )
# MAGIC -- using delta
# MAGIC -- location '/Path_of_parquet_blob/Silver/MasterData/TB_CRM_EXTERNAL_REFERENCE_KEY'

# COMMAND ----------

# spark.sql(" insert into master_data_dev.SILVER_TB_CRM_BADGE select * from vw_badge_df")