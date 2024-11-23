# Databricks notebook source
# DBTITLE 1,Imports and references
import dlt
from pyspark.sql.functions import expr, lit, col, udf
from pyspark.sql.types import StringType

volume_folder = "/Volumes/hagedata/lumen_network_planning/traffic_landing/"


# COMMAND ----------

# DBTITLE 1,Ports Rules
rules = {} # create empty dictionary for rules
rules["no_cui_ports"] = "port NOT IN (200, 201)" #add rules to dictionary
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values())) # filter-able statement for pipe


# COMMAND ----------

# DBTITLE 1,Create Streaming Table from Raw XML
@dlt.table(table_properties={"quality": "bronze"}) #can add many properties, just an example
@dlt.expect_all(rules) # expect_all imposes all rules in the rules dict, add column is_quarantined for any rows that violate the rules
def traffic_bronze_dlt_st():
  return (
     spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rootTag", "hierarchy")
        .option("rowTag", "row")
        .load(volume_folder).withColumn("is_quarantined", expr(quarantine_rules) )
  )


# COMMAND ----------

# DBTITLE 1,Split Quarantined Rows
@dlt.table(
  name="invalid_traffic"
)
def invalid_traffic():
  return (
    dlt.read("traffic_bronze_dlt_st")
      .filter("is_quarantined=true")
      .withColumn("ip_address", lit("[REDACTED]"))
      .withColumn("url", lit("[REDACTED]"))
      .withColumn("user_agent", lit("[REDACTED]"))
  )

def write_invalid_traffic():
  dlt.write("invalid_traffic", dlt.read("invalid_traffic"))

# COMMAND ----------

# DBTITLE 1,Read clean IP Location Table


@dlt.table(table_properties={"quality":"bronze", "mergeSchema":"true"})
def geo_lookup_dlt():
  return spark.table("hagedata.lumen_network_planning.geo_ip_clean")

# COMMAND ----------

# DBTITLE 1,Streaming: Keep Only Non-CUI Rows, Join to Location Lookup
@dlt.table(
  comment = "ip lookup"
)
@dlt.expect_all(rules)
def ip_loc_lookup_dlt_st():
  traffic = dlt.readStream("traffic_bronze_dlt_st").filter("is_quarantined=false")
  locs = dlt.read("geo_lookup_dlt").withColumnRenamed("ip_address", "ip_address_lookup")
  return (traffic
          .join(locs, traffic.ip_address==locs.ip_address_lookup, 'inner'))
  


# COMMAND ----------

# DBTITLE 1,Streaming: Look up Postal Code

@dlt.table(
  comment="Lookup postal code from latitude and longitude"
)
def zip_lookup():
  geo_dir = '/Volumes/hagedata/lumen_network_planning/geodata'
  geo = spark.read.format('csv').option('header', 'true').load(geo_dir).select("latitude", "longitude", "postal_code")
  ip_loc = dlt.readStream("ip_loc_lookup_dlt_st")
  
  return ip_loc.join(
    geo,
    (ip_loc.lat == geo.latitude) & (ip_loc.lon == geo.longitude),
    "left"
  ).select(ip_loc["*"], geo["postal_code"])

# COMMAND ----------

# DBTITLE 1,Materialized View of Aggregates
@dlt.table(comment="Traffic data by zip")
def traffic_by_zip():
  return dlt.read("zip_lookup").groupBy("postal_code").count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Future Improvements
# MAGIC - Set up an Unsupervised ML model to observe which values are like the CUI rows to help pre-emptively identify new rules to quarantine those values
# MAGIC - [Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html) to automate the above
# MAGIC - CI/CD for the pipeline with test instances to ensure reproducibility
# MAGIC - [Lakeview Dashboards](https://www.databricks.com/blog/announcing-public-preview-lakeview-dashboards) for lightweight analytics on output data
# MAGIC
