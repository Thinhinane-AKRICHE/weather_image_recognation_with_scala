package com.weather.domain

case class WeatherImageRecord(
  imagePath: String,
  fileName: String,
  label: String,
  labelId: Int,
  width: Int,
  height: Int,
  channels: Int,
  features: Seq[Float]
)