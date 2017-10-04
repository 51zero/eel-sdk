package io.eels.cloudera

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import scala.collection.JavaConverters._
import scalaj.http.{Http, HttpResponse}

case class Role(name: String, `type`: String, roleState: String)
case class Service(name: String, `type`: String)
case class ConfigFile(name: String)

class ClouderaSupport(uri: URI) extends Logging {

  private val apiVersion = "v4"

  private val (user, pass) = uri.getUserInfo.split(':') match {
    case Array(u, p) => (u, p)
  }

  val clusterUrl = s"http://${uri.getHost}:${uri.getPort}/api/$apiVersion/clusters${uri.getPath}"
  logger.info(s"Cloudera API will be accessed at $clusterUrl")

  val serviceListUrl = clusterUrl + "/services"

  private def rolesUrl(serviceName: String): String = serviceListUrl + "/" + serviceName + "/roles"

  private def configFilesUrl(serviceName: String, roleName: String): String = rolesUrl(serviceName) + "/" + roleName + "/process"

  private def configFileUrl(serviceName: String, roleName: String, file: String): String = {
    configFilesUrl(serviceName, roleName) + "/configFiles/" + file
  }

  private def services: Seq[Service] = {
    val response: HttpResponse[String] = Http(serviceListUrl).auth(user, pass).asString
    val json = JacksonSupport.mapper.readTree(response.body)
    json.get("items").elements().asScala.map { node =>
      Service(node.get("name").textValue, node.get("type").textValue)
    }.toVector
  }

  private def roles(serviceName: String): Seq[Role] = {
    val response: HttpResponse[String] = Http(rolesUrl(serviceName)).auth(user, pass).asString
    val json = JacksonSupport.mapper.readTree(response.body)
    json.get("items").elements().asScala.map { node =>
      Role(node.get("name").textValue, node.get("type").textValue, node.get("roleState").textValue)
    }.toVector
  }

  private def configFiles(serviceName: String, roleName: String): Seq[ConfigFile] = {
    val response: HttpResponse[String] = Http(configFilesUrl(serviceName, roleName)).auth(user, pass).asString
    val json = JacksonSupport.mapper.readTree(response.body)
    json.get("configFiles").elements().asScala.map(_.textValue).map(ConfigFile.apply).toVector
  }

  private def fetchFile(serviceName: String, roleName: String, file: String): String = {
    Http(configFileUrl(serviceName, roleName, file)).auth(user, pass).asString.body
  }

  private def hiveSiteXml: String = fetchContents("HIVE", "HIVESERVER2", "hive-site.xml")
  private def coreSiteXml: String = fetchContents("HDFS", "NAMENODE", "core-site.xml")

  private def fetchContents(serviceName: String, roleName: String, fileName: String): String = {
    logger.debug(s"Fetching configuration for $serviceName/$roleName/$fileName")

    val _services = services
    logger.debug(s"_services=${_services}")

    val _service = _services.find(_.`type` == serviceName).getOrError(s"Could not find $serviceName service in services ${_services}")

    val _roles = roles(_service.name)
    logger.debug(s"_roles=${_roles}")

    val _role = _roles.find(_.`type` == roleName).getOrError(s"Could not find role=$roleName in roles ${_roles}")

    val _configFiles = configFiles(_service.name, _role.name)
    logger.debug(s"_configFiles=${_configFiles}")

    val file = _configFiles.find(_.name.contains(fileName)).getOrError(s"Could not find $fileName")
    fetchFile(_service.name, _role.name, file.name)
      .replaceAll("\\{\\{.*?\\}\\}", "") // replace any env placeholders as they won't be resolved here
  }

  def conf: Configuration = {
    val conf = new Configuration(false)
    conf.addResource(new ByteArrayInputStream(coreSiteXml.getBytes(StandardCharsets.UTF_8.name)))
    conf
  }

  def hiveConf: HiveConf = {
    val conf = new HiveConf()
    conf.addResource(new ByteArrayInputStream(coreSiteXml.getBytes(StandardCharsets.UTF_8.name)))
    conf.addResource(new ByteArrayInputStream(hiveSiteXml.getBytes(StandardCharsets.UTF_8.name)))
    conf
  }
}

object JacksonSupport {

  val mapper: ObjectMapper with ScalaObjectMapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
  mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
  mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
  mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
}
