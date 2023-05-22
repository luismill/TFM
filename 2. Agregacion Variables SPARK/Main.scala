package com.grupotsk
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp, to_utc_timestamp, udf}

import java.util.UUID
import scala.util.control.Breaks.break

object Main extends App {
  // Declaración de variables
  val cas = new Cassandra
  val config = cas.config() // Configuración de Cassandra
  val keyspace = "tecnologia_energia"
  val tableToRead = "medidas" // Tabla de cassandra de la que se lee 
  val tableToWrite = "agregados_descarga" // Tabla de cassandra en la que se guarda
  val dat = new Date
  val initialDate = "2017-01-06T00:00:00" // Fecha inicial de la consulta en formato "yyyy-MM-ddT00:00:00"
  val finalDate = "2018-12-31T00:00:00" // Fecha final de la consulta en formato "yyyy-MM-ddT00:00:00"
  val agg = new Aggregation
  val duration = "1 minute" // Duración de la agregación
  val lista_kks = List(
    "11CXA01GZ001XK05_A.PV_Out#Value", "11CXA02GZ001XK05_A.PV_Out#Value", "11CXA01GZ001XK07_A.PV_Out#Value",
    "11CXA02GZ001XK07_A.PV_Out#Value", "11CXA01GZ001XK06_A.PV_Out#Value", "11CXA02GZ001XK06_A.PV_Out#Value",
    "11CXA01GZ001XK08_A.PV_Out#Value", "11CXA02GZ001XK08_A.PV_Out#Value", "11CXA01GZ001XK09_A.PV_Out#Value",
    "11CXA02GZ001XK09_A.PV_Out#Value", "13WSX40CF001JT01_A.PV_Out#Value", "13WSC10CL001JT2X_X.PV_Out#Value",
    "13WSH10CL001JT2X_X.PV_Out#Value", "13WSC_MEDIA_M0.PV_Out#Value", "13WSH_MEDIA_M0.PV_Out#Value",
    "13WSX10CT001JT3X_X.PV_Out#Value", "13WSX40CT001JT3X_X.PV_Out#Value", "13WSH10AA080MCV_V.RbkOut#Value",
    "13WSC10AA080MCV_V.RbkOut#Value", "12WTD30CT001JT2X_X.PV_Out#Value", "12WTD81CT001JT2X_X.PV_Out#Value",
    "12WTD81CF001JT01_A.PV_Out#Value", "14MAY30DS901XU85_A.PV_Out#Value", "SUBESTACION_Potencia_Activa",
    "15HAD13CP001JT2X_X.PV_Out#Value", "15HAD13CP002JT2X_X.PV_Out#Value", "12WTB10CF001JT2X_X.PV_Out#Value",
    "15PAB20CF001JT01_A.PV_Out#Value", "12WTB35AA080MV_V.RbkOut#Value"
  )

  // Configuración SPARK y conexión con CASSANDRA
  // Creación de la sesión
  val spark = SparkSession.builder
    .master("spark: XXXXXXXXXXX ")
    .appName("Agregacion")
    .withExtensions(new CassandraSparkExtensions)
    .config(config)
    .getOrCreate()
  import spark.implicits._ // Permite el uso de $ para acceder a columnas
  spark.sparkContext.setLogLevel("WARN") // Solo se muestran mensajes de advertencia y errores

  // Creación de una lista con las fechas
  val listaFechas = dat.toList(initialDate, finalDate)

  for (kks <- lista_kks){
    val datos_raw = cas.readRawDataToUTC(tableToRead, keyspace, listaFechas, kks)
    val agregados = agg.avg(datos_raw, duration)

    agregados.show(10)
    cas.saveAggUTCDfToTable(agregados, tableToWrite, keyspace)
  }
 
  spark.stop()
}

