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
cdp_dw_score_path = '/Path_of_parquet_blob/cdp3_scoresBase.parquet'
cdp_dw_StringMapBase_path = '/Path_of_parquet_blob/StringMapBase.parquet'
cdp_dw_Entity_path = '/Path_of_parquet_blob/Entity.parquet'
cdp_dw_questionnaire_path = '/Path_of_parquet_blob/cdp3_questionnaireBase.parquet'

df_Theme = spark.read.format('delta').load("./DIM_THEME")


# Reading the parquet files
cdp_dw_score_df = read_parquet_data(cdp_dw_score_path)
cdp_dw_questionnaire_df = read_parquet_data(cdp_dw_questionnaire_path)

cdp_dw_StringMapBase_df = read_parquet_data(cdp_dw_StringMapBase_path)
cdp_dw_StringMapBase_df = cdp_dw_StringMapBase_df.drop(cdp_dw_StringMapBase_df.OrganizationId)

cdp_dw_Entity_df = read_parquet_data(cdp_dw_Entity_path)

# Drop duplicates
cdp_dw_score_df_initial=cdp_dw_score_df.dropDuplicates()
cdp_dw_questionnaire_df = cdp_dw_questionnaire_df.dropDuplicates()
cdp_dw_StringMapBase_df=cdp_dw_StringMapBase_df.dropDuplicates()
cdp_dw_Entity_df=cdp_dw_Entity_df.dropDuplicates()
df_Theme=df_Theme.dropDuplicates()

# COMMAND ----------

# Join condition for "CRM_STATUS_REASON" and "SCORE_TYPE_NAME" columns

# "JOIN dbo.StringMapBase String on 
# T.statuscode = String.AttributeValue and 
# String.AttributeName = 'statuscode' and 
# String.ObjectTypeCode = (SELECT ObjectTypeCode FROM dbo.Entity where OverwriteTime = 0 AND ComponentState = 0 and   
# name like 'cdp3_Scores')"

# "JOIN
# dbo.StringMapBase String1 on 
# T.statuscode = String1.AttributeValue and 
# String1.AttributeName = 'cdp3_scoretype' and 
# String1.ObjectTypeCode = (SELECT ObjectTypeCode FROM dbo.Entity where OverwriteTime = 0 AND ComponentState = 0 and   
# name like 'cdp3_Scores')"


cdp_dw_Entity_df = cdp_dw_Entity_df.filter("OverwriteTime = '1900-01-01 00:00:00.000' AND ComponentState = 0")
cdp_dw_Entity_df = cdp_dw_Entity_df.filter(col("Name").contains("cdp3_scores")).select("ObjectTypeCode")

ObjTypeCode_Val=cdp_dw_Entity_df.collect()[0][0]

statuscode_StringMapBase_df = cdp_dw_StringMapBase_df.select(
  cdp_dw_StringMapBase_df.AttributeValue.alias('AttributeValue_1'),
  cdp_dw_StringMapBase_df.AttributeName.alias('AttributeName_1'),
  cdp_dw_StringMapBase_df.ObjectTypeCode.alias('ObjectTypeCode_1'),
  cdp_dw_StringMapBase_df.Value.alias('Value_statuscode')
)
scoretype_StringMapBase_df = cdp_dw_StringMapBase_df.select(
  cdp_dw_StringMapBase_df.AttributeValue.alias('AttributeValue_2'),
  cdp_dw_StringMapBase_df.AttributeName.alias('AttributeName_2'),
  cdp_dw_StringMapBase_df.ObjectTypeCode.alias('ObjectTypeCode_2'),
  cdp_dw_StringMapBase_df.Value.alias('Value_scoretype')
)

cdp_dw_score_df_initial_join = cdp_dw_score_df_initial.join(
  statuscode_StringMapBase_df,
  (
    cdp_dw_score_df_initial.statuscode == statuscode_StringMapBase_df.AttributeValue_1
  ) & (
    statuscode_StringMapBase_df.AttributeName_1 == "statuscode"
  ) & (
    statuscode_StringMapBase_df.ObjectTypeCode_1 == ObjTypeCode_Val),
  "left").join(
  scoretype_StringMapBase_df, 
  (
    cdp_dw_score_df_initial.statuscode == scoretype_StringMapBase_df.AttributeValue_2
  ) & (
    scoretype_StringMapBase_df.AttributeName_2 == "cdp3_scoretype"
  ) & (
    scoretype_StringMapBase_df.ObjectTypeCode_2 == ObjTypeCode_Val),
  "left")


# COMMAND ----------

# Generate UUID for new column
# Create UDF function to create surrogate key

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# Create a dataframe with required columns
UTCTIME = getArgument('UTCTIME')
#UTCTIME = '2021-12-02 12:25:32.870'
score_df=cdp_dw_score_df_initial_join.select(
    cdp_dw_score_df_initial_join.cdp3_scoresId.alias("CRM_SCORE_ID"),
    cdp_dw_score_df_initial_join.cdp3_name.alias("SCORE_NAME"),
    cdp_dw_score_df_initial_join.cdp3_questionnaire.alias("CRM_QUESTIONNAIRE_ID"),
    cdp_dw_score_df_initial_join.cdp3_organization.alias("CRM_ORGANIZATION_ID"),
    cdp_dw_score_df_initial_join.cdp3_Invitation.alias("CRM_INVITATION_ID"),
    cdp_dw_score_df_initial_join.statecode.alias("CRM_STATUS"),
    cdp_dw_score_df_initial_join.Value_statuscode.alias("CRM_STATUS_REASON"),
    cdp_dw_score_df_initial_join.cdp3_scoretype.alias("CRM_SCORE_TYPE"),
    cdp_dw_score_df_initial_join.cdp3_score.alias("CRM_SCORE"),
    cdp_dw_score_df_initial_join.Value_scoretype.alias("SCORE_TYPE_NAME"),
    cdp_dw_score_df_initial_join.CreatedOn.alias("CRM_CREATED_ON"),
    cdp_dw_score_df_initial_join.ModifiedOn.alias("CRM_MODIFIED_ON"),
).withColumn("dw_created_on",lit(UTCTIME)).withColumn("dw_modified_on",lit(UTCTIME)).withColumn("DATESK",current_timestamp()).withColumn("DATE",current_timestamp()).withColumn("IS_DELETED",lit(0)).withColumn("SCORE_ID",uuidUdf())

score_df.createOrReplaceTempView("vw_scores_data")
cdp_dw_questionnaire_df.createOrReplaceTempView("vw_questioner_year")
df_Theme.createOrReplaceTempView("vw_df_Theme")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_questioner_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.SCORE_ID,a.CRM_SCORE_ID,a.CRM_SCORE_TYPE,a.CRM_INVITATION_ID,a.CRM_ORGANIZATION_ID,a.CRM_QUESTIONNAIRE_ID,a.CRM_STATUS,a.SCORE_NAME,a.CRM_CREATED_ON,a.CRM_MODIFIED_ON,a.dw_created_on,a.dw_modified_on,a.DATESK,a.DATE,a.CRM_STATUS_REASON,a.SCORE_TYPE_NAME,b.cdp3_questionnaireyear,b.
# MAGIC cdp3_themeid,c.THEME_NAME,b.cdp3_name
# MAGIC from vw_scores_data a
# MAGIC left join vw_questioner_year b
# MAGIC on a.CRM_QUESTIONNAIRE_ID=b.cdp3_questionnaireId
# MAGIC left join vw_df_Theme c
# MAGIC on c.CRM_THEME_ID=b.cdp3_themeid

# COMMAND ----------

display(score_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_df_dim_score_final AS
# MAGIC (
# MAGIC select a.SCORE_ID,a.CRM_SCORE_ID,a.CRM_SCORE_TYPE,a.CRM_INVITATION_ID,a.CRM_ORGANIZATION_ID,a.CRM_QUESTIONNAIRE_ID,a.CRM_STATUS,a.SCORE_NAME,a.CRM_CREATED_ON,a.CRM_MODIFIED_ON,a.dw_created_on,a.dw_modified_on,a.DATESK,a.DATE,a.CRM_STATUS_REASON,a.SCORE_TYPE_NAME,b.cdp3_questionnaireyear,b.
# MAGIC cdp3_themeid,c.THEME_NAME,b.cdp3_name
# MAGIC from vw_scores_data a
# MAGIC left join vw_questioner_year b
# MAGIC on a.CRM_QUESTIONNAIRE_ID=b.cdp3_questionnaireId
# MAGIC left join vw_df_Theme c
# MAGIC on c.CRM_THEME_ID=b.cdp3_themeid
# MAGIC 
# MAGIC )

# COMMAND ----------

# Read from Delta & Write to parquet
df_scores_delta = spark.read.format('delta').load("/Path_of_parquet_blob/CRM/DIM_SCORE")
df_scores_delta.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/PostDataset/SILVER_DIM_SCORE')


# COMMAND ----------

# # Read from Delta & Write to parquet

# df_scores_delta = spark.read.format('delta').load("/Path_of_parquet_blob/MasterData/TB_CRM_SCORES")
# df_waterMark= spark.read.format('parquet').load('/mnt/root/CDP/Development/Config/Silver_waterMark/SILVER_TB_CRM_SCORES_WATERMARK')

# WaterMark=df_waterMark.collect()[0][0]
# df_scores_delta_incremented = df_scores_delta.filter(df_scores_delta.DW_MODIFIED_DATE >= WaterMark)

# df_scores_delta_incremented.write.mode("overwrite").format("parquet").save('/Path_of_parquet_blob/PostDataset/SILVER_TB_CRM_SCORES')

# WaterMark_theme_max_modified_dt = df_scores_delta.select(max(df_scores_delta.DW_MODIFIED_DATE).alias('MaxModifiedOn'))
# WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/mnt/root/CDP/Development/Config/Silver_waterMark/SILVER_TB_CRM_SCORES_WATERMARK')

# COMMAND ----------

# df_theme_delta = spark.read.format('delta').load("/Path_of_parquet_blob/MasterData/TB_CRM_SCORES")
# WaterMark_theme_max_modified_dt = df_theme_delta.select(max(df_theme_delta.DW_MODIFIED_DATE).alias('MaxModifiedOn'))
# WaterMark_theme_max_modified_dt.write.mode("overwrite").format("parquet").save('/mnt/root/CDP/Development/Config/Silver_waterMark/SILVER_TB_CRM_SCORES_WATERMARK')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_dev.SILVER_DIM_SCORE --where CRM_SCORE_TYPE is not null
# MAGIC --truncate table silver_dev.SILVER_DIM_SCORE
# MAGIC 
# MAGIC -- select * from vw_scores_data
# MAGIC 
# MAGIC -- SELECT COUNT(1)  FROM master_data_dev.SILVER_TB_CRM_SCORES
# MAGIC --drop table silver_dev.SILVER_DIM_SCORE
# MAGIC -- delete from master_data_dev.SILVER_TB_CRM_SCORES
# MAGIC 
# MAGIC --TRUNCATE TABLE master_data_dev.SILVER_TB_CRM_SCORES
# MAGIC 
# MAGIC -- CREATE TABLE silver_dev.SILVER_DIM_SCORE
# MAGIC -- (
# MAGIC -- SCORE_ID string,
# MAGIC -- CRM_SCORE_ID string,
# MAGIC -- score string,
# MAGIC -- CRM_SCORE_TYPE string,
# MAGIC -- crm_score_type_code string,
# MAGIC -- CRM_INVITATION_ID string,
# MAGIC -- CRM_ORGANIZATION_ID string,
# MAGIC -- CRM_QUESTIONNAIRE_ID string,
# MAGIC -- crm_questionnaire_name string,
# MAGIC -- crm_questionnaire_year  string,
# MAGIC -- CRM_STATUS int,
# MAGIC -- SCORE_NAME string,
# MAGIC -- theme_id string,
# MAGIC -- crm_theme_name string,
# MAGIC -- CRM_CREATED_ON timestamp,
# MAGIC -- CRM_MODIFIED_ON timestamp,
# MAGIC -- dw_created_on timestamp,
# MAGIC -- dw_modified_on timestamp,
# MAGIC -- DATESK timestamp,
# MAGIC -- DATE timestamp,
# MAGIC -- CRM_STATUS_REASON string,
# MAGIC -- SCORE_TYPE_NAME string
# MAGIC 
# MAGIC -- )
# MAGIC -- using delta
# MAGIC -- location '/Path_of_parquet_blob/CRM/DIM_SCORE'

# COMMAND ----------

# spark.sql(" insert into master_data_dev.SILVER_TB_CRM_SCORES select * from vw_scores_data")

# COMMAND ----------

