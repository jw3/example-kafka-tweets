package wiii

import akka.actor._
import akka.stream.ActorMaterializer
import com.softwaremill.embeddedkafka.EmbeddedKafka

import scala.concurrent.Await
import scala.concurrent.duration.Duration


/**
 * start up kafka
 * fire up the writer, which reads a twitter stream and writes it to kafka
 * fire up the reader, which reads the kafka stream and prints it to stdout
 *
 * derived from the software mill activator at
 *   https://github.com/softwaremill/activator-reactive-kafka-scala
 *
 */
object Driver extends App {
    implicit val actorSystem: ActorSystem = ActorSystem("TwitterToKafka")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val topic = "test_topic"
    val kafka = actorSystem.actorOf(Props[EmbeddedKafka])
    val writer = actorSystem.actorOf(KafkaWriter.props(topic))
    val reader = actorSystem.actorOf(KafkaReader.props(topic))

    Await.ready(actorSystem.whenTerminated, Duration.Inf)
}
