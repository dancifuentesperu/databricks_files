# Databricks notebook source
###############################################
#          Author: Daniel Cifuentes           #
#          Last update: 22/06/2025            #
###############################################

# Definir nombres de catalog, schema, volume 

catalog_name        = "integration_catalog"
schema_name_raw     = "raw_files"
volume_name_raw     = "files"
volume_name_tmp     = "tmp"
schema_name_tables  = "raw_tables"


# COMMAND ----------

# Crear catálogo
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
""")

# Crear schemas
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name_raw}
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name_tables}
""")

# Crear volumen
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name_raw}.{volume_name_raw}
""")

# Crear volumen tmp
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name_raw}.{volume_name_tmp}
""")

# COMMAND ----------


#Visualizar los volumes del schema 
df_vols_raw = spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema_name_raw}")
df_vols_raw.show()

# Describe schema 
df_schema_describe = spark.sql(f"DESCRIBE SCHEMA {catalog_name}.{schema_name_raw}")
df_schema_describe.show()

#Mostrar tablas del schema raw_tables 
df_tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name_tables}")
df_tables.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nro 5
# MAGIC DESCRIBE METASTORE