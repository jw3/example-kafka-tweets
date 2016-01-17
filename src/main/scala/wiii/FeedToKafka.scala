package wiii

import akka.actor._
import akka.stream.actor.ActorSubscriber
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import com.softwaremill.react.kafka.{ProducerMessage, ProducerProperties, ReactiveKafka}
import twitter4j.{Status, _}


object KafkaWriter {
    def props(topic: String)(implicit mat: ActorMaterializer) = Props(new KafkaWriter(topic))
}

class KafkaWriter(topicName: String)(implicit mat: Materializer) extends Actor with ActorLogging {
    override def preStart(): Unit = initWriter()
    override def receive: Receive = {
        case _ =>
    }

    def initWriter(): Unit = {
        val subscriberProps = new ReactiveKafka().producerActorProps(ProducerProperties(
            bootstrapServers = "localhost:9092",
            topic = topicName,
            valueSerializer = TweetSerializer
        ))
        val subscriber = context.actorOf(subscriberProps)
        val (actorRef, publisher) = Source.actorRef[Status](1000, OverflowStrategy.fail).toMat(Sink.asPublisher(false))(Keep.both).run()

        val factory = new TwitterStreamFactory()
        val twitterStream = factory.getInstance()
        twitterStream.addListener(new StatusForwarder(actorRef))
        twitterStream.filter(new FilterQuery("espn"))

        Source.fromPublisher(publisher).map(s => ProducerMessage(Tweet(s.getUser.getName, s.getText)))
        .runWith(Sink.fromSubscriber(ActorSubscriber[ProducerMessage[Array[Byte], Tweet]](subscriber)))
    }
}

class StatusForwarder(publisher: ActorRef) extends StatusListener {
    def onStatus(status: Status): Unit = publisher ! status

    //\\ nop all the others for now  //\\
    def onStallWarning(warning: StallWarning): Unit = {}
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
    def onException(ex: Exception): Unit = {}
}



