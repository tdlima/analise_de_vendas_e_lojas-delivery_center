# Databricks notebook source
# MAGIC %md
# MAGIC #SQL Para Engenharia de Dados
# MAGIC
# MAGIC ##Criação do Banco de Dados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carregamento da Base de Dados
# MAGIC
# MAGIC ###Carregando arquivo CSV com a tabela Orders e realizando criação do dataframe com o Spark.

# COMMAND ----------

df_orders = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/juniorlima@impulsoda.onmicrosoft.com/orders.csv")
df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Salvando a estrutura de tabela no Database

# COMMAND ----------

df_orders.write.format("delta").mode("append").saveAsTable("bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Realizando o mesmo processo com a tabela Stores

# COMMAND ----------

df_stores = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/juniorlima@impulsoda.onmicrosoft.com/stores.csv")
df_stores.display()

# COMMAND ----------

df_stores.write.format("delta").mode("append").saveAsTable("bronze.stores")

# COMMAND ----------

# MAGIC %md
# MAGIC #Trabalhando com SQL
# MAGIC
# MAGIC ##SELECT, WHERE, DISTINCT, JOIN E LIMIT

# COMMAND ----------

# MAGIC %md
# MAGIC ##Realizando consultas no Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, order_status FROM bronze.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filtrando a Consulta
# MAGIC
# MAGIC ###Buscando registro pelo Status 'FINISHED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders WHERE order_status = 'FINISHED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.stores

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando todos os valores em 'store_segment'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(store_segment) FROM bronze.stores

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limitando a quantidade de registros na busca

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.stores LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unindo tabelas para análise
# MAGIC
# MAGIC ###Consultando o Identificador e o Amount em Orders, verificando junto o Store Name e ordenando pelo Amout de forma decrescente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   orders.order_id,
# MAGIC   orders.order_amount,
# MAGIC   stores.store_name
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC INNER JOIN
# MAGIC   bronze.stores
# MAGIC ON
# MAGIC   orders.store_id = orders.store_id
# MAGIC ORDER BY orders.order_amount DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Análise de Custos
# MAGIC
# MAGIC ###Cálculando a taxa e o custo das entregas para saber o valor total dos custos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   store_id,
# MAGIC   order_delivery_fee,
# MAGIC   order_delivery_cost,
# MAGIC round((order_delivery_fee + order_delivery_cost), 2) AS total_cost
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC WHERE
# MAGIC   order_delivery_fee > 0
# MAGIC AND
# MAGIC   order_delivery_cost IS NOT NULL
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ###Obtendo a soma total das vendas de determinada loja

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   store_id,
# MAGIC   ROUND(SUM(order_amount), 2) as TOTAL
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC WHERE
# MAGIC   store_id = '786'
# MAGIC GROUP BY
# MAGIC   store_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contando a quantidade de lojas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT(store_id)) FROM bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criando um novo dataframe para simulação de dados duplicados

# COMMAND ----------

orders2 = spark.sql("SELECT * FROM bronze.orders LIMIT 100")
display(orders2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicando os dados no database

# COMMAND ----------

orders2.write.format("delta").mode("append").saveAsTable("bronze.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##Retornando registros duplicados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, store_id, order_amount, COUNT(*) AS records FROM bronze.orders GROUP BY order_id, store_id, order_amount

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contagem geral de registros duplicados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(*)
# MAGIC FROM(
# MAGIC   SELECT order_id, store_id, order_amount, COUNT(*) AS records FROM bronze.orders GROUP BY order_id, store_id, order_amount
# MAGIC ) a
# MAGIC WHERE a.records > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Resolvendo o problema de duplicidade dos dados
# MAGIC
# MAGIC ###Sera criado um novo database que recebera os dados limpos e, em seguida, sera realizado o processo de limpeza dos dados e a ingestão desses dados no novo database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE prata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE prata.orders AS (SELECT DISTINCT * FROM bronze.orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prata.orders LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)  FROM prata.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##Detectando Anomalias como valores negativos ou grandes demais

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ntile,
# MAGIC   min(order_amount) AS limite_inferior,
# MAGIC   max(order_amount) AS limite_superior,
# MAGIC   avg(order_amount) AS media,
# MAGIC   count(order_id) AS orders
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT order_id, cast(order_amount AS FLOAT),
# MAGIC     ntile(4) OVER (ORDER BY cast(order_amount AS FLOAT)) AS ntile
# MAGIC     FROM prata.orders
# MAGIC   ) a
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prata.orders WHERE cast(order_amount as FLOAT) > 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ##Manipulação de Dados
# MAGIC
# MAGIC ###

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(channel_id) FROM prata.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ###Channel_id não possui nenhuma informaçao que faça referencia aos canais. Vamos acrescentar uma coluna com essa informação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   store_id,
# MAGIC   order_amount,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN channel_id = "11" THEN "APP"
# MAGIC       WHEN channel_id = "15" THEN "SITE"
# MAGIC       WHEN channel_id = "17" THEN "WHATSAPP"
# MAGIC       ELSE "MARKETPLACE"
# MAGIC     END
# MAGIC   ) AS channel
# MAGIC   FROM prata.orders
# MAGIC   WHERE
# MAGIC     channel_id IN ("11","15","17")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Manipulando tipos de dados
# MAGIC
# MAGIC ###Convertendo o Order Amount em Float

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CAST(order_amount AS FLOAT)
# MAGIC FROM
# MAGIC   prata.orders
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CAST(order_amount AS FLOAT) AS total_price,
# MAGIC   "19,99" as base_price,
# MAGIC   CAST(REPLACE(REPLACE(REPLACE("R$ 19,99", "R$","")," ",""),",",".") AS FLOAT) AS total_price_formated
# MAGIC FROM
# MAGIC   prata.orders
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Formatando datas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_created_day,
# MAGIC   order_created_month,
# MAGIC   order_moment_created
# MAGIC FROM
# MAGIC   prata.orders
# MAGIC WHERE
# MAGIC   order_created_day >= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_moment_created,
# MAGIC   TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9)," ",""),"M/d/yyyy") AS order_moment_created_formated
# MAGIC FROM
# MAGIC   prata.orders
# MAGIC WHERE
# MAGIC   order_created_day > 10
# MAGIC AND
# MAGIC   order_created_month > 2
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tratando dados ausentes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_amount,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         order_delivery_cost IS NULL AND order_amount > 0 THEN ROUND((SELECT AVG(order_delivery_cost)
# MAGIC         FROM bronze.orders), 2)
# MAGIC       ELSE order_delivery_cost
# MAGIC     END
# MAGIC   ) AS order_delivery_cost
# MAGIC FROM
# MAGIC   bronze.orders
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criando nova tabela com dados tratados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE prata.orders_amount_store AS (SELECT
# MAGIC   order_moment_date,
# MAGIC   store_name,
# MAGIC   ROUND(SUM(order_amount), 2) AS total
# MAGIC FROM
# MAGIC   (SELECT
# MAGIC     TO_DATE(REPLACE(SUBSTRING(order_moment_created, 1, 9)," ",""),"M/d/yyyy") AS order_moment_date,
# MAGIC     stores.store_name,
# MAGIC     CAST(order_amount AS FLOAT) AS order_amount
# MAGIC   FROM
# MAGIC     bronze.orders
# MAGIC   INNER JOIN
# MAGIC     bronze.stores
# MAGIC   ON
# MAGIC     stores.store_id = orders.store_id
# MAGIC   WHERE
# MAGIC     order_moment_created IS NOT NULL
# MAGIC   AND
# MAGIC     CAST(order_amount AS FLOAT) <= 10000)
# MAGIC GROUP BY
# MAGIC   order_moment_date, store_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prata.orders_amount_store

# COMMAND ----------


