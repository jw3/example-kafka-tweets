package wiii

import akka.actor._
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration.DurationInt


object KafkaReader {
    def props(topic: String)(implicit mat: ActorMaterializer) = Props(new KafkaReader(topic))
}

class KafkaReader(topicName: String)(implicit mat: Materializer) extends Actor with LazyLogging {
    override def preStart(): Unit = initReader()
    override def receive: Receive = {
        case _ =>
    }

    def initReader(): Unit = {
        val consumer = new ReactiveKafka().consumeWithOffsetSink(ConsumerProperties(
            bootstrapServers = "localhost:9092",
            topic = topicName,
            groupId = "groupId",
            valueDeserializer = TweetDeserializer
        ).commitInterval(1000 milliseconds))(context.system)

        Source.fromPublisher(consumer.publisher).map(processMessage).to(consumer.offsetCommitSink).run()
        logger.debug("started kafka reader")
    }

    def processMessage(msg: ConsumerRecord[Array[Byte], Tweet]) = {
        println(msg.value())
        msg
    }
}