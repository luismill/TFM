package com.grupotsk
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.collection.mutable.ListBuffer

object Main extends App {
  // Configuración SPARK y conexión con CASSANDRA
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "enertech-data-001.tsk.cloud")
    .set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .set("spark.cores.max", "94") // Núcleos máximos a utilizar (En el submit se indican 30)
    .set("spark.executor.cores", "23") // Núcleos por executor (En el submit se indican 12)
    .set("spark.executor.memory", "29g") // Memoria por executor (En el submit se indican 15g)

  // Creación de la sesión
  val spark = SparkSession.builder
    .master("spark://enertech-compute-001.tsk.cloud:7077")
    // .master("spark://clustertsk-107.grupotsk.com:7077")
    .appName("Agregacion")
    .withExtensions(new CassandraSparkExtensions)
    .config(conf)
    .getOrCreate()
  import spark.implicits._ // Permite el uso de $ para acceder a columnas
  spark.sparkContext.setLogLevel("WARN") // Solo se muestran mensajes de advertencia y errores

  val datos_df = spark.read.table("mycatalog.tecnologia_energia.shagayaetl") // Lectura de la tabla

  // Creación de una lista con los kks a leer. Potencia bruta y temperatura ambiente en la estación central
  val lista_kks = List(
    "AS11NProgram_DATA_SF_10S._11CXA01CT001TP01","AS11NProgram_DATA_SF_10S._11CXA02CT001TP01", "AS11NProgram_DATA_SF_10S._11CXA03CT001TP01", // Temperatura ambiente
    "AS11NProgram_DATA_SF_1MIN._11CXA01CM001XP01", "AS11NProgram_DATA_SF_1MIN._11CXA02CM001XP01", "AS11NProgram_DATA_SF_1MIN._11CXA03CM001XP01", // Humedad relativa
    "AS11NProgram_DATA_SF_10S._11CXA01CS001SP01", "AS11NProgram_DATA_SF_10S._11CXA02CS001SP01", "AS11NProgram_DATA_SF_10S._11CXA03CS001SP01", // Velocidad de viento
    "AS11NProgram_DATA_SF_1MIN._11CXA01CG001ZP01", "AS11NProgram_DATA_SF_1MIN._11CXA02CG001ZP01", "AS11NProgram_DATA_SF_1MIN._11CXA03CG001ZP01", // Dirección de viento
    "13WSX10CF001_KG_S_X.PV_Out#Value", "13WSX16CF001_KG_S_X.PV_Out#Value", // Flujo sales
    "13WSC10CL001_X.PV_Out#Value", "13WSH10CL001_X.PV_Out#Value", // Niveles tanques de sales
    "AVERAGE_COLD_X.PV_Out#Value", "AVERAGE_HOT_X.PV_Out#Value",  // Temperatura tanques de sales
    "13WSX10CT001_X.PV_Out#Value", "13WSX16CT006_X.PV_Out#Value", // Temperatura sales HX
    "13WSH20AA090_V.RbkOut#Value", "13WSC20AA090_V.RbkOut#Value", // Válvulas carga/descarga sales
    "12WTD16CT002_X.PV_Out#Value", "12WTD11CT001_X.PV_Out#Value", // Temperatura HTF HX
    "12WTD11CF001_X.PV_Out#Value",  // Flujo HTF HX
    "AS10Program_DATA_SAE_1MIN._10BAY10_XQ14", "AS10Program_DATA_SAE_1MIN._10BAY40_XQ50_EXPORT",  // Potencia bruta y neta
    "15LBA60CT001_X.PV_Out#Value", "15HAD12CP002_X.PV_Out#Value", // Temperatura y presión calderín 1
    "15LBA70CT001_X.PV_Out#Value", "15HAD16CP001_X.PV_Out#Value"  // Temperatura y presión calderín 2
  )

  // Los instantes tienen que estar en formato Timestamp y en UTC para que cuadren con los timestamp de Cassandra
  val fecha_inicial = LocalDateTime.of(2018, 7, 3, 0, 0).atZone(ZoneId.of("UTC"))
  val instante_inicial = fecha_inicial.toInstant
  val fecha_final = LocalDateTime.of(2020, 11, 11, 0, 0).atZone(ZoneId.of("UTC"))
  val instante_final = fecha_final.toInstant

  // Creación de una lista con todas las fechas a obtener
  val lista_fechas: ListBuffer[Instant] = ListBuffer.empty[Instant]
  var instante_actual = instante_inicial
  while (!instante_actual.isAfter(instante_final)) {
    lista_fechas += instante_actual
    instante_actual = instante_actual.plusSeconds(86400) // Se mueve el instante_actual 1 día
  }

  // Esquema del dataframe es igual al esquema de la tabla original de Cassandra
  val schema = StructType(List(
    StructField("kks", StringType),
    StructField("date", TimestampType),
    StructField("time", TimestampType),
    StructField("value", DoubleType)
  ))

  for (kks <- lista_kks) {
    // Inicialización de 1 dataframe vacío para ir uniendo los datos
    var datos_dias = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    // Bucle por la lista de fechas para hacer consultas sobre las partition key.
    for (fecha <- lista_fechas) {
      val datos_dia = datos_df
        .filter(col("kks") === kks) // filtro del kks
        .filter(col("date") === fecha) // filtro del día
      datos_dias = datos_dias.union(datos_dia) // Cada resultado se une al dataframe global
    }

    // Cambio de la hora a UTC desde la local de Europe/Madrid para obtener la original de Cassandra
    val datos_utc = datos_dias
      .withColumn("time_utc", to_utc_timestamp($"time", "Europe/Madrid"))

    // Se agregan los resultados, promediando cada minuto
    val agregados = datos_utc
      .groupBy($"kks", window($"time_utc", "1 minute"))
      .avg()
      .sort("window")

    // Formateado de los agregados en la tabla
    val agregados_formateado = agregados
      .withColumn("time", $"window.start") // La columna 'time' tiene que tener formato timestamp. Se selecciona el timestamp inicial del periodo promediado.

    agregados_formateado.show(10)

    // Se guardan los datos en la tabla spark_agregados_descarga_shagaya
    agregados_formateado
      .select($"kks", $"time", $"avg(value)".as("value"))
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "spark_agregados_descarga_shagaya", "keyspace" -> "tecnologia_energia"))
      .mode("append")
      .save()
  }
  spark.stop()
}
