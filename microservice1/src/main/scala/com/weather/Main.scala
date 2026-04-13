package com.weather

import org.apache.log4j.{Level, Logger}

import com.weather.domain.WeatherImageRecord
import com.weather.extractor.ImageFileScanner
import com.weather.parquet.ParquetExporter
import com.weather.transform.{ImagePreprocessor, LabelEncoder}

import java.io.File
import scala.util.{Failure, Success, Try}

object Main {


  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("io.native.lib.available", "false")
    System.setProperty("hadoop.native.lib", "false")

    val rawDir = "../dataset"
    val outputParquet = "../processed/weather.parquet"

    println("=== START PARSER ===")

    val imageFiles = ImageFileScanner.scanImages(rawDir)
    println(s"[INFO] ${imageFiles.size} images trouvées")

    if (imageFiles.isEmpty) {
      println("[ERROR] Aucun fichier trouvé")
      return
    }

    val labels = imageFiles.map(extractLabel)
    val labelMap = LabelEncoder.buildLabelMap(labels)

    println(s"[INFO] Labels: $labelMap")

    val records = imageFiles.flatMap { file =>
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

    println(s"[INFO] ${records.size} records valides")

    if (records.nonEmpty) {
      new File("../data/processed").mkdirs()
      ParquetExporter.write(outputParquet, records)
      println(s"[SUCCESS] Parquet généré : $outputParquet")
    } else {
      println("[ERROR] Aucun record à écrire")
    }

    println("=== END PARSER ===")
  }

  private def extractLabel(file: File): String = {
    file.getParentFile.getName.toLowerCase
  }
}