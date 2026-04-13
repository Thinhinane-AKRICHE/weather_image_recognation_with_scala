package com.weather.parquet

import com.weather.domain.WeatherImageRecord
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object ParquetExporter {

  def write(
      spark: SparkSession,
      outputPath: String,
      records: Seq[WeatherImageRecord],
      mode: SaveMode
  ): Unit = {

    import spark.implicits._

    val ds = spark.createDataset(records)(Encoders.product[WeatherImageRecord])

    ds.coalesce(1)
      .write
      .mode(mode)
      .parquet(outputPath)
  }
}