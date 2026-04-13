package com.weather.parquet

import com.weather.domain.WeatherImageRecord
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object ParquetExporter {

  def write(outputPath: String, records: Seq[WeatherImageRecord]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeatherRecognition")
      .master("local[*]")
      // --- AJOUT DES LIGNES CI-DESSOUS ---
      .config("spark.driver.memory", "4g")    // Alloue 4 Go au Driver (celui qui gère ta liste de records)
      .config("spark.executor.memory", "4g")  // Alloue 4 Go aux exécuteurs
      .config("spark.driver.maxResultSize", "2g") // Pour éviter les crashs lors du transfert de gros datasets
      // ----------------------------------
      .config("spark.hadoop.io.nativeio.disable", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    try {
      import spark.implicits._

      // Ton code reste le même...
      val ds = spark.createDataset(records)(Encoders.product[WeatherImageRecord])

      ds.write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath)

    } finally {
      spark.stop()
    }
  }
}