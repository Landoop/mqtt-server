package com.datamountaineer.mqtt.server

import java.io.{BufferedWriter, FileOutputStream, FileWriter}
import java.nio.ByteBuffer

import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.moquette.proto.messages.{AbstractMessage, PublishMessage}
import io.moquette.server.Server
import io.moquette.server.config.ClasspathConfig

object Program extends App {

/*  val schema = SchemaFor[TemperatureMeasure]().toString(true)
  val w = new BufferedWriter(new FileWriter("temperaturemeasure.avro"))
  w.write(schema)
  w.flush()
  w.close()
  */
  val jsonTopic = "/mjson"
  val avroTopic = "/mavro"
  val classPathConfig = new ClasspathConfig()

  val connection = "tcp://0.0.0.0:11883"
  val qs = 1
  var mqttBroker = new Server()
  mqttBroker.startServer(classPathConfig)

  println("Starting mqtt service on port 11883")

  val tempatures = Seq(
    TemperatureMeasure(1, 31.1, "EMEA", System.currentTimeMillis()),
    TemperatureMeasure(2, 30.91, "EMEA", System.currentTimeMillis()),
    TemperatureMeasure(3, 30.991, "EMEA", System.currentTimeMillis()),
    TemperatureMeasure(4, 31.061, "EMEA", System.currentTimeMillis()),

    TemperatureMeasure(101, 27.001, "AMER", System.currentTimeMillis()),
    TemperatureMeasure(102, 38.001, "AMER", System.currentTimeMillis()),
    TemperatureMeasure(103, 26.991, "AMER", System.currentTimeMillis()),
    TemperatureMeasure(104, 34.17, "AMER", System.currentTimeMillis())
  )

  println(s"Hit Enter to start publishing messages on topic: $jsonTopic and $avroTopic")
  scala.io.StdIn.readLine()

  val recordFormat = RecordFormat[TemperatureMeasure]
  tempatures.zipWithIndex.foreach { case (t, i) =>
    println(s"Publishing message $i ...")
    publishMessage(jsonTopic, JacksonJson.toJson(t).getBytes)
    publishMessage(avroTopic, AvroSerializer.getBytes(t))
    Thread.sleep(200)
  }

  println("Hit Enter to complete...")
  scala.io.StdIn.readLine()
  mqttBroker.stopServer()


  private def publishMessage(topic: String, payload: Array[Byte]) = {
    val message = new PublishMessage()
    message.setTopicName(s"$topic")
    message.setRetainFlag(false)
    message.setQos(AbstractMessage.QOSType.EXACTLY_ONCE)
    message.setPayload(ByteBuffer.wrap(payload))
    mqttBroker.internalPublish(message)
  }
}
