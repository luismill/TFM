package com.grupotsk
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.collection.mutable.ListBuffer

class Date {
  def toList (initialDate: String, finalDate: String): ListBuffer[Instant] = {
    // Las fechas tienen que estar en formato Timestamp y en UTC para que cuadren con los timestamp de Cassandra
    val fechaInicial = LocalDateTime.parse(initialDate).atZone(ZoneId.of("UTC"))
    val instanteInicial = fechaInicial.toInstant
    val fechaFinal = LocalDateTime.parse(finalDate).atZone(ZoneId.of("UTC"))
    val instanteFinal = fechaFinal.toInstant

    // Creación de una lista con todas las fechas a obtener
    val listaFechas: ListBuffer[Instant] = ListBuffer.empty[Instant]
    var instanteActual = instanteInicial
    while (!instanteActual.isAfter(instanteFinal)) {
      listaFechas += instanteActual
      instanteActual = instanteActual.plusSeconds(86400) // Se mueve el instante_actual 1 día hacia delante
    }
    listaFechas
  }

}
