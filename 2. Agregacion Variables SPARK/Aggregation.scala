package com.grupotsk
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.grupotsk.Main.spark


class Aggregation {
  def avg (df: DataFrame, windowDuration: String): DataFrame = {
    import spark.implicits._ // Permite el uso de $ para acceder a columnas
    val agg_avg = df.
      groupBy($"kks", window ($"time", windowDuration))
      .avg()
      .sort("window")
    agg_avg
  }
}
