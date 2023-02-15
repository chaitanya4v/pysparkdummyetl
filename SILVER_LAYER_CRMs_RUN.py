# Databricks notebook source
import json
from pyspark.sql.functions import*
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import uuid

# COMMAND ----------

UTCTIME = getArgument('UTCTIME')

notebook_dependencies=dbutils.notebook.run('./DIM_SCORE',6000,{"UTCTIME": UTCTIME})

notebook_dependencies=dbutils.notebook.run('./TB_CRM_EXTERNAL_REFERENCE_KEY',6000,{"UTCTIME": UTCTIME})

notebook_dependencies=dbutils.notebook.run('./FACT_SCORE_REGIONS',6000,{"UTCTIME": UTCTIME})
