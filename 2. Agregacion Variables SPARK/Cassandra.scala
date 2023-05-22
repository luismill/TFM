package com.grupotsk
import com.grupotsk.Main.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.cassandra.{AlwaysOn, DataFrameReaderWrapper}
import com.datastax.spark.connector._
import java.time.Instant
import scala.collection.mutable.ListBuffer

class Cassandra {
  def config(): SparkConf = {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "enertech-data-001.tsk.cloud")
      .set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .set("spark.cores.max", "94") // Núcleos máximos a utilizar (En el submit se indican 30)
      .set("spark.executor.cores", "23") // Núcleos por executor (En el submit se indican 12)
      .set("spark.executor.memory", "29g") // Memoria por executor (En el submit se indican 15g)
    conf
  }

  def readRawDataToUTC(tableToRead: String, keyspace: String, listaFechas: ListBuffer[Instant], kks: String): DataFrame = {
    // Función que lee los datos brutos en Cassandra de un kks, tablas con datos particionados por días

    // Esquema del dataframe basado en las partition key de las tablas de datos raw, kks y date
    val schema_pk = StructType(
      Seq(
        StructField("k", StringType, nullable = false),
        StructField("d", TimestampType, nullable = false),
      )
    )

    val pks = listaFechas.map(fecha => Row(kks, Timestamp.from(fecha))) // Lista con las filas de partition keys
    val pksDf = spark.createDataFrame(spark.sparkContext.parallelize(pks), schema_pk) // Df de las partition keys
    val datosDf = spark.read.cassandraFormat(tableToRead, keyspace).load // Lectura de la tabla de Cassandra

    // Direct join entre df con partition keys y tabla de cassandra
    val datos = pksDf.join(datosDf.directJoin(AlwaysOn), pksDf("k") === datosDf("kks") && pksDf("d") === datosDf("date"))

    import spark.implicits._ // Permite el uso de $ para acceder a columnas

    // Cambio de la hora a UTC desde la local de Europe/Madrid para obtener la original de Cassandra
    val datosUTC = datos.withColumn("time_utc", to_utc_timestamp($"time", "Europe/Madrid"))

    // Solo se devuelve el kks, el tiempo en UTC y el valor. No se devuelve el date.
    datosUTC.select($"kks", $"date", $"time_utc".as("time"), $"value")
  }

  
  def saveAggUTCDfToTable(df: DataFrame, tableToWrite: String, keyspace: String): Unit = {
    import spark.implicits._ // Permite el uso de $ para acceder a columnas
    val dfToSave = df.withColumn("time", $"window.start") // La columna 'time' tiene que tener formato timestamp. Se selecciona el timestamp inicial del periodo promediado.
      .withColumn("time_local", from_utc_timestamp($"time", "Europe/Madrid"))

    dfToSave.select($"kks", $"time_local".as("time"), $"avg(value)".as("value"))
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableToWrite, "keyspace" -> keyspace))
      .mode("append")
      .save()
  }
}
