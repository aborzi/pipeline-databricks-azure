// Databricks notebook source
# Conferindo se os dados foram montados e se temos acesso a pasta inbound

// COMMAND ----------

// MAGIC %python
// MAGIC dbuntils.fs.ls("/mnt/dados/inbound")
// MAGIC

// COMMAND ----------

Lendo os dados na camada de inbound

// COMMAND ----------

val path = "dbfs: /mnt/dados/inbound/dados brutos imoveis.json"
val dados = spark.read.json(path)


// COMMAND ----------

display(dados)


// COMMAND ----------

removendo colunas

// COMMAND ----------

val dados anuncio dados.drop("imagens", "usuario")
display(dados_anuncio)


// COMMAND ----------



// COMMAND ----------

# criando uma coluna de identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col


// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)


// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

