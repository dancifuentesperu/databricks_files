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

# BRONZE
schema_name_bronze  = "bronze_layer"
table_name_bronze   = "titanic_bronze"
# SILVER
schema_name_silver  = "silver_layer"
table_name_silver   = "titanic_silver"
# GOLD
schema_name_gold  = "gold_layer"
table_name_gold   = "titanic_gold"

# schema 
# workspace_folder       = "lab4"


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

## Crear los esquemas de bronze, silver y gold 

## Este script llevar al main 
## Creación de esquemas Bronze, silver y gold 

full_schema_path_bronze = f"{catalog_name}.{schema_name_bronze}"
full_schema_path_silver = f"{catalog_name}.{schema_name_silver}"
full_schema_path_gold   = f"{catalog_name}.{schema_name_gold}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_path_bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_path_silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_path_gold}")

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
