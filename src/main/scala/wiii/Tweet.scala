package wiii

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import spray.json.{DefaultJsonProtocol, _}


case class Tweet(user: String, text: String)

object TweetProtocol extends DefaultJsonProtocol {
    implicit val currencyRteFormat = jsonFormat2(Tweet)
}

object TweetSerializer extends Serializer[Tweet] {
    import wiii.TweetProtocol._

    override def serialize(s: String, t: Tweet): Array[Byte] =
        t.toJson.compactPrint.getBytes("UTF-8")

    override def close(): Unit = {}
    override def configure(map: util.Map[String, _], b: Boolean): Unit = {}
}

object TweetDeserializer extends Deserializer[Tweet] {
    import wiii.TweetProtocol._

    override def deserialize(s: String, bytes: Array[Byte]): Tweet =
        new String(bytes, "UTF-8").parseJson.convertTo[Tweet]


    override def close(): Unit = {}
    override def configure(map: util.Map[String, _], b: Boolean): Unit = {}
}