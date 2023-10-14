// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")
// MAGIC

// COMMAND ----------

Lendo os dados na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)


// COMMAND ----------

display(df)


// COMMAND ----------

Transformando os campos do json em coluna

// COMMAND ----------

display(df.select("anuncio.*"))


// COMMAND ----------

(
  df.select("anuncio.*", "anuncio.endereco.*")
)


// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")


// COMMAND ----------

Removendo colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)


// COMMAND ----------

Salvando na camada silver

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

