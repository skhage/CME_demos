# Databricks notebook source
# MAGIC %md
# MAGIC # Updating Raw Data with Redactions
# MAGIC The DLT pipeline we created writes quarantined data to a table. We can use this information to find which raw XML files have CUI. This script opens those files, finds the `event_id` that matches CUI, replaces columns `ip_address`, `url`, and `user_agent` with `<REDACTED>`.
# MAGIC
# MAGIC When the DLT pipeline runs again, no CUI will have been in the existing XML files, but this process repeats in case of new violations.
# MAGIC
# MAGIC Finally, the quarantined table is dropped. Now there is no record of CUI in Databricks.

# COMMAND ----------

# DBTITLE 1,Read Raw Files with File Name
from pyspark.sql.functions import input_file_name
import re
import pandas as pd

df = spark.read.format("xml").option("rowTag", "row").load("/Volumes/hagedata/lumen_network_planning/traffic_landing/").select("*", "_metadata.file_path")

df.display()

# COMMAND ----------

# DBTITLE 1,Load Invalid Traffic
redact = spark.table("hagedata.lumen_network_planning.invalid_traffic")
redact.display()

# COMMAND ----------

# DBTITLE 1,Find Relevant Files and Event_ID
updates = df.join(redact, df['event_id'] == redact['event_id'], how = 'inner').select(df['file_path'], df['event_id'])
update_files = updates.select('file_path').distinct().collect()
update_files = [row['file_path'] for row in update_files]
update_files = [re.sub('dbfs:', '', x) for x in update_files]

update_ids = updates.select('event_id').distinct().collect()
update_ids = [row['event_id'] for row in update_ids]


# COMMAND ----------

# DBTITLE 1,Overwrite Raw XML with Redactions
redact_pd = redact.toPandas()
for file in update_files:
  update_xml = pd.read_xml(file)
  update_xml.loc[update_xml.event_id.isin(update_ids),['ip_address', 'url', 'user_agent']] = ['<REDACTED>']*3
  update_xml.to_xml(file, index=False)

# COMMAND ----------

# DBTITLE 1,Drop Invalid Traffic Table
# MAGIC %sql drop table if exists hagedata.lumen_network_planning.invalid_traffic
