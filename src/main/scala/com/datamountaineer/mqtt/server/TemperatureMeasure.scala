package com.datamountaineer.mqtt.server

case class TemperatureMeasure(deviceId: Int, value: Double, region: String, timestamp: Long)
