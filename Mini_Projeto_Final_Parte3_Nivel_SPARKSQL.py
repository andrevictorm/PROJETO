# Databricks notebook source
# DBTITLE 1,Criando estrutura
df = (spark.read.format("csv")
         .option("inferschema", True)
         .option("header", True)
         .option("sep", ",")
         .load("/FileStore/tables/markting_campedpyspark-5.csv")
         .createOrReplaceTempView("mkt")
      )

# COMMAND ----------

# DBTITLE 1,Visualizando o BD
# MAGIC %sql
# MAGIC SELECT * FROM mkt

# COMMAND ----------

Escolhi fazer essa analise para identificar se o nivel de formação influencia na compra.
A consulta irá somar os valores "Soma_Total_Mnt" e da coluna "Total_Compra_Realizada" e agrupar de acordo com a formação

# COMMAND ----------

# DBTITLE 1,Soma dos valores separados por nivel academico
# MAGIC %sql
# MAGIC SELECT Formacao,
# MAGIC SUM(Soma_Total_Mnt) as Soma_Formacao_Mnt,
# MAGIC SUM(Total_Compra_Realizada) as Total_Formacao_Compra
# MAGIC FROM mkt
# MAGIC GROUP BY Formacao

# COMMAND ----------

Esse comando tem o objetivo de analisar por qual via os clientes mais realizam compras de acordo com seu grau de instrução.
A consulta irá somar os valores das colunas "NumCompra" e agrupar de acordo com a formação

# COMMAND ----------

# DBTITLE 1,Compras efetuadas por formação
# MAGIC %sql
# MAGIC SELECT Formacao, 
# MAGIC SUM(NumCompraPromocao) AS Compra_Promocao_Realizada,
# MAGIC SUM(NumCompraWeb) AS Compra_Web_Realizada,
# MAGIC SUM(NumCompraCatalogo) AS Compra_Catalogo_Realizada,
# MAGIC SUM(NumCompraLoja) AS Compra_Loja_Realizada
# MAGIC 
# MAGIC FROM mkt
# MAGIC GROUP BY Formacao
# MAGIC ORDER BY Formacao

# COMMAND ----------

Esse comando tem o objetivo de analisar a quantidade de clientes que compram separando por grau de instrução
A consulta irá contar os valores das colunas "Formacao" e agrupar de acordo com a formação

# COMMAND ----------

# DBTITLE 1,Quantidade de pessoas formadas 
# MAGIC %sql
# MAGIC SELECT Formacao ,COUNT(Formacao) as Formacao_quantidade
# MAGIC FROM mkt
# MAGIC GROUP BY Formacao

# COMMAND ----------

Esse comando tem o objetivo de analisar a quantidade media de clientes que compram separando por grau de instrução
A consulta irá calcular as medias das colunas "Total_Compra_Realizada" e "Soma_Total_Mnt", e agrupar de acordo com a formação

# COMMAND ----------

# DBTITLE 1,Media de Compra realizada por nivel de formação
# MAGIC %sql
# MAGIC SELECT Formacao,
# MAGIC AVG(Total_Compra_Realizada) Media_Total_Compra,
# MAGIC AVG(Soma_Total_Mnt) AS Media_Total_Mnt
# MAGIC 
# MAGIC FROM mkt
# MAGIC GROUP BY Formacao
# MAGIC ORDER BY Formacao

# COMMAND ----------

Esse comando tem o objetivo de analisar a quantidade de clientes que compram separando por estado civil
A consulta irá contar os valores das colunas "estado_civil" e agrupar de acordo com a formação

# COMMAND ----------

# DBTITLE 1,Quantidade de pessoas separadas por estado civil
# MAGIC %sql
# MAGIC SELECT estado_civil ,COUNT(estado_civil ) as estadocivil_quantidade
# MAGIC FROM mkt
# MAGIC GROUP BY estado_civil 

# COMMAND ----------

Esse comando tem o objetivo de analisar por qual via os clientes mais realizam compras de acordo com seu estado civil.
A consulta irá somar os valores das colunas "NumCompra" e agrupar de acordo com o estado civil do cliente

# COMMAND ----------

# DBTITLE 1,Soma das compras realizadas separadas por estado civil
# MAGIC %sql
# MAGIC SELECT estado_civil, 
# MAGIC SUM(NumCompraPromocao) AS Compra_Promocao,
# MAGIC SUM(NumCompraWeb) AS Compra_Web,
# MAGIC SUM(NumCompraCatalogo) AS Compra_Catalogo,
# MAGIC SUM(NumCompraLoja) AS Compra_Loja
# MAGIC 
# MAGIC FROM mkt
# MAGIC GROUP BY estado_civil
# MAGIC ORDER BY estado_civil

# COMMAND ----------

Esse comando tem o objetivo de analisar a quantidade media de clientes que compram separando por estado civil
A consulta irá calcular as medias das colunas "Total_Compra_Realizada" e "Soma_Total_Mnt", e agrupar de acordo com o estado civil

# COMMAND ----------

# DBTITLE 1,Media de compra por estado civil
# MAGIC %sql
# MAGIC SELECT estado_civil,
# MAGIC AVG(Total_Compra_Realizada) AS Media_Compra,
# MAGIC AVG(Soma_Total_Mnt) AS Media_Mnt
# MAGIC 
# MAGIC FROM mkt
# MAGIC GROUP BY estado_civil
# MAGIC ORDER BY estado_civil

# COMMAND ----------

Esse comando tem o objetivo de analisar os clientes que possuem rendar maior que 5mil e seu poder de compra
A consulta usa o Where para exibir apenas os clientes que possuem renda maior que 5mil

# COMMAND ----------

# DBTITLE 1,Renda maior que 5mil
# MAGIC %sql
# MAGIC SELECT * FROM mkt
# MAGIC WHERE renda > 5000;

# COMMAND ----------

Esse comando tem o objetivo de analisar a quantidade de clientes que possuem rendar maior que 5mil agrupado pelo estado civil
A consulta usa o Where para exibir apenas os clientes que possuem renda maior que 5mil, o Count irá contar a quantidade de pessoas que atendem esse perfil agrupando pelo estado civil e ordenando pela Quantidade do mesmo, do menor para o maior

# COMMAND ----------

# DBTITLE 1,Quantidade de pessoas que tem renda maior que 5mil por estado civil
# MAGIC %sql
# MAGIC SELECT estado_civil ,COUNT(estado_civil ) as Qnt_estadocivil
# MAGIC FROM mkt
# MAGIC WHERE renda > 5000
# MAGIC GROUP BY estado_civil 
# MAGIC ORDER BY Qnt_estadocivil

# COMMAND ----------

Esse comando tem o objetivo de analisar a quantidade de clientes que possuem rendar maior que 5mil agrupado pela formação
A consulta usa o Where para exibir apenas os clientes que possuem renda maior que 5mil, o Count irá contar a quantidade de pessoas que atendem esse perfil agrupando pela formação e ordenando pela Quantidade do mesmo, do menor para o maior

# COMMAND ----------

# DBTITLE 1,Quantidade de pessoas que tem renda maior que 5mil por Formação
# MAGIC %sql
# MAGIC SELECT Formacao ,COUNT(Formacao) as Qnt_Formacao
# MAGIC FROM mkt
# MAGIC WHERE renda > 5000
# MAGIC GROUP BY Formacao 
# MAGIC ORDER BY Qnt_Formacao
