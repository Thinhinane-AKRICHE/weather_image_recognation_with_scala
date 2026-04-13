package com.weather.extractor

import java.io.File

object ImageFileScanner {

  private val allowedExtensions = Set("jpg", "jpeg", "png", "bmp")

  def scanImages(rootDir: String): Seq[File] = {
    val root = new File(rootDir)

    if (!root.exists() || !root.isDirectory) {
      throw new IllegalArgumentException(s"Le dossier n'existe pas ou n'est pas un dossier : $rootDir")
    }

    listFilesRecursively(root).filter(isImageFile)
  }

  private def listFilesRecursively(file: File): Seq[File] = {
    val children = Option(file.listFiles()).getOrElse(Array.empty[File])
    children.flatMap { child =>
      if (child.isDirectory) listFilesRecursively(child)
      else Seq(child)
    }.toSeq
  }

  private def isImageFile(file: File): Boolean = {
    val name = file.getName.toLowerCase
    allowedExtensions.exists(ext => name.endsWith("." + ext))
  }
}