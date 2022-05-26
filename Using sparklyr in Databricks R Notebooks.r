# Databricks notebook source
# MAGIC %md # Use `sparklyr` in Databricks R notebooks
# MAGIC 
# MAGIC This notebook shows how to use sparklyr in Databricks notebooks.

# COMMAND ----------

# MAGIC %md ## Load `sparklyr` package

# COMMAND ----------

# this is a comment
library(sparklyr)

# COMMAND ----------

# MAGIC %md ## Create a `sparklyr` connection
# MAGIC 
# MAGIC Use `"databricks"` as the connection method in `spark_connect()`.
# MAGIC No additional parameters to ``spark_connect()`` are required. You do not need to call `spark_install()` as Spark is already installed on the Databricks cluster.
# MAGIC 
# MAGIC Note that `sc` is a special name for `sparklyr` connection. When you use that variable name, the notebook automatically displays Spark progress bars and built-in Spark UI viewers.

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md ## Use `sparklyr` and `dplyr` APIs
# MAGIC 
# MAGIC After setting up the `sparklyr` connection, you can use the `sparklyr` API.
# MAGIC You can import and combine `sparklyr` with `dplyr` or `MLlib`.  
# MAGIC If you use an extension package that includes third-party JARs, you may need to install those JARs as libraries in your workspace ([AWS](https://docs.databricks.com/libraries/workspace-libraries.html#workspace-libraries)|[Azure](https://docs.microsoft.com/azure/databricks/libraries/workspace-libraries#workspace-libraries)).

# COMMAND ----------

library(dplyr)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at everything made available in this workspace:

# COMMAND ----------

src_databases(sc)

# COMMAND ----------

src_tbls(sc)

# COMMAND ----------

# MAGIC %md
# MAGIC We can load data in from the data lake with Spark using Sparklyr's `spark_read_table`function.

# COMMAND ----------

tbl_change_db(sc, "stuart")
src_tbls(sc)

# COMMAND ----------

?spark_read_table

# COMMAND ----------

airlineDf <- spark_read_table(sc, "airlines_extract")

# COMMAND ----------

# MAGIC %md
# MAGIC To take a look at a sample of these rows, we can use dataframe -> head -> collect -> display.

# COMMAND ----------

airlineDf %>% head(1000) %>% collect() %>% display()

# COMMAND ----------

# MAGIC %md ## Transformation, aggregation and visualization
# MAGIC 
# MAGIC All of these operations can be achieved using standard `dplyr` syntax, e.g.

# COMMAND ----------

library(lubridate)

cancelledDayAirlineDf <- airlineDf %>%
  mutate(date = make_date(Year, Month, DayofMonth)) %>%
  group_by(date, UniqueCarrier) %>%
  summarise(
    flights = n(),
    cancelled = sum(Cancelled, na.rm=TRUE)
  ) %>%
  arrange(date, UniqueCarrier)

# COMMAND ----------

cancelledDayAirlineDf %>% collect() %>% display()

# COMMAND ----------

# MAGIC %md ## Plotting with ggplot

# COMMAND ----------

library(ggplot2)

# Change the default plot height 
options(repr.plot.height = 600)

# COMMAND ----------

cancelledDayAirlineDf %>%
  ggplot(aes(x=date, y=cancelled, fill=UniqueCarrier)) +
  geom_bar(stat="identity") +
  theme(legend.position="top")

# COMMAND ----------

# MAGIC %md
# MAGIC To move data from R into Spark, we can use dplyr's standard `copy_to` function.

# COMMAND ----------

iris_tbl <- copy_to(sc, iris, overwrite=TRUE)

# COMMAND ----------

print(class(iris))
print(class(iris_tbl))

# COMMAND ----------

iris_tbl %>% 
  filter(Species == "setosa") %>% 
  count

# COMMAND ----------

# MAGIC %md ## Execute SQL statements

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   * EXCEPT (CRSDepTime, CRSArrTime)
# MAGIC FROM stuart.airlines
# MAGIC WHERE UniqueCarrier = "XE"

# COMMAND ----------

sdf_sql(sc, 
  "SELECT
    * EXCEPT (CRSDepTime, CRSArrTime)
  FROM stuart.airlines_extract
  WHERE UniqueCarrier = \"XE\""
  ) %>% 
  collect() %>%
  display()

# COMMAND ----------

# MAGIC %md ## Code to create the datasets.

# COMMAND ----------

# MAGIC %python
# MAGIC schema = spark.read.csv("dbfs:/databricks-datasets/airlines/part-00000", header=True, inferSchema=True).schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists stuart

# COMMAND ----------

# MAGIC %python
# MAGIC (
# MAGIC   spark.readStream
# MAGIC   .format("csv")
# MAGIC   .schema(schema)
# MAGIC #   .option("maxFilesPerTrigger", 10)
# MAGIC   .load("dbfs:/databricks-datasets/airlines", header=False, inferSchema=False)
# MAGIC   .writeStream
# MAGIC   .format("delta")
# MAGIC   .outputMode("append")
# MAGIC   .trigger(once=True)
# MAGIC   .partitionBy("Year", "Month", "DayofMonth")
# MAGIC   .option("checkpointLocation", "/home/stuart/checkpoints/airlines_delta")
# MAGIC   .start("/home/stuart/datasets/airlines_delta")
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists stuart.airlines
# MAGIC using delta
# MAGIC location '/home/stuart/datasets/airlines_delta'

# COMMAND ----------

# MAGIC %sql optimize stuart.airlines

# COMMAND ----------

# MAGIC %sql select * from stuart.airlines

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col
# MAGIC (
# MAGIC   spark.read
# MAGIC   .table("stuart.airlines")
# MAGIC   .where(col("Year") == 2008)
# MAGIC   .where(col("Month") == 12)
# MAGIC   .write
# MAGIC   .mode("append")
# MAGIC #   .option("checkpointLocation", "/home/stuart/checkpoints/airlines_extract")
# MAGIC   .save("/home/stuart/datasets/airlines_extract")
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists stuart.airlines_extract
# MAGIC using delta
# MAGIC location '/home/stuart/datasets/airlines_extract'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stuart.airlines_extract
