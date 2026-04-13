package com.weather

import com.weather.domain.WeatherImageRecord
import com.weather.extractor.ImageFileScanner
import com.weather.parquet.ParquetExporter
import com.weather.transform.{ImagePreprocessor, LabelEncoder}

import java.io.File
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("io.native.lib.available", "false")
    System.setProperty("hadoop.native.lib", "false")

    // Évite OneDrive pour les entrées/sorties Spark
    val rawDir = "C:/spark-data/dataset"
    val outputDir = "C:/spark-data/output"
    val outputParquet = s"$outputDir/weather.parquet"

    val batchSize = 100

    println("=== START PARSER ===")

    new File(outputDir).mkdirs()
    deleteRecursively(new File(outputParquet))

    val imageFiles = ImageFileScanner.scanImages(rawDir)
    println(s"[INFO] ${imageFiles.size} images trouvées")

    if (imageFiles.isEmpty) {
      println("[ERROR] Aucun fichier trouvé")
      return
    }

    val labels = imageFiles.map(extractLabel)
    val labelMap = LabelEncoder.buildLabelMap(labels)

    println(s"[INFO] Labels: $labelMap")

    val spark = SparkSession.builder()
      .appName("WeatherRecognition")
      .master("local[1]")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set(
      "mapreduce.fileoutputcommitter.algorithm.version",
      "2"
    )
    spark.sparkContext.hadoopConfiguration.set(
      "fs.file.impl",
      "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    spark.sparkContext.hadoopConfiguration.set(
      "fs.file.impl.disable.cache",
      "true"
    )

    try {
      val batches = imageFiles.grouped(batchSize).toSeq
      var totalValidRecords = 0

      batches.zipWithIndex.foreach { case (batch, batchIndex) =>
        println(s"[INFO] Batch ${batchIndex + 1}/${batches.size} - ${batch.size} images")

        val records: Seq[WeatherImageRecord] = batch.flatMap { file =>
          val label = extractLabel(file)
          val labelId = labelMap(label)

          Try {
            val (width, height, channels, features) =
              ImagePreprocessor.preprocess(file)

            WeatherImageRecord(
              imagePath = file.getAbsolutePath,
              fileName = file.getName,
              label = label,
              labelId = labelId,
              width = width,
              height = height,
              channels = channels,
              features = features
            )
          } match {
            case Success(record) =>
              println(s"[OK] ${file.getName}")
              Some(record)

            case Failure(e) =>
              println(s"[ERROR] ${file.getName} -> ${e.getMessage}")
              None
          }
        }

        println(s"[INFO] Batch ${batchIndex + 1}: ${records.size} records valides")
        totalValidRecords += records.size

        if (records.nonEmpty) {
          val mode =
            if (batchIndex == 0) SaveMode.Overwrite
            else SaveMode.Append

          try {
            ParquetExporter.write(spark, outputParquet, records, mode)
            println(s"[INFO] Batch ${batchIndex + 1} écrit dans le parquet")
          } catch {
            case e: Exception =>
              println(s"[ERROR] Échec écriture batch ${batchIndex + 1} -> ${e.getMessage}")
              throw e
          }
        }
      }

      println(s"[INFO] Total records valides : $totalValidRecords")

      if (totalValidRecords > 0) {
        println(s"[SUCCESS] Parquet généré : $outputParquet")
      } else {
        println("[ERROR] Aucun record à écrire")
      }

    } finally {
      spark.stop()
    }

    println("=== END PARSER ===")
  }

  private def extractLabel(file: File): String = {
    file.getParentFile.getName.toLowerCase
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        val children = file.listFiles()
        if (children != null) {
          children.foreach(deleteRecursively)
        }
      }
      file.delete()
    }
  }
}