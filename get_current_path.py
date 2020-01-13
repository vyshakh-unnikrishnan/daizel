# Databricks notebook source
# MAGIC %md 
# MAGIC # This code snippet can be used to see the full path
# MAGIC In python we need to  use a scala snippet
# MAGIC 
# MAGIC https://kb.databricks.com/notebooks/get-notebook-path.html

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.text("notebook", dbutils.notebook.getContext().notebookPath.get)

# COMMAND ----------

dbutils.widgets.get("notebook")

# COMMAND ----------

