package io.eels.component.kafka

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.SourceParser
import io.eels.component.Builder

object KafkaSourceParser extends SourceParser {
  // eg kafka:localhost:9000,localhost:9001/topic1[a,b,c],topic2
  val UrlRegex = "kafka:(.+?)/(.+?)(\\?.*)?".r
  val HostPortRegex = "(.+):(\\d+)".r
  override def apply(url: String): Option[Builder[KafkaSource]] = url match {
    case UrlRegex(brokerList, topicstr, params) =>
      val topics = topicstr.split(',').toSet
      Some(KafkaSourceBuilder(brokerList, topics, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class KafkaSourceBuilder(brokerList: String, topics: Set[String], params: Map[String, List[String]])
  extends Builder[KafkaSource] {
  require(topics.nonEmpty, "Must specify at least one topic")
  override def apply(): KafkaSource = {
    val consumerGroup = params.getOrElse("consumerGroup", List("eel")).head
    val config = KafkaSourceConfig(brokerList, consumerGroup)
    KafkaSource(config, topics, JsonKafkaDeserializer)
  }
}