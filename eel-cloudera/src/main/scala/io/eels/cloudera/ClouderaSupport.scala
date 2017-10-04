package io.eels.cloudera

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.collection.JavaConverters._

import scalaj.http.{Http, HttpResponse}

case class Role(name: String, `type`: String, roleState: String)
case class Service(name: String, `type`: String)
case class ConfigFile(name: String)

object ClouderaSupport extends App {

  private val hostname = "ec2-35-177-30-57.eu-west-2.compute.amazonaws.com"
  private val managerPort = 7180
  private val apiVersion = "v4"
  private val clusterName = "cluster1"
  private val user = "admin"
  private val pass = "admin"

  val clusterUrl = s"http://$hostname:$managerPort/api/$apiVersion/clusters/$clusterName"
  val serviceListUrl = clusterUrl + "/services"

  def rolesUrl(serviceName: String): String = serviceListUrl + "/" + serviceName + "/roles"

  def configFilesUrl(serviceName: String, roleName: String): String = rolesUrl(serviceName) + "/" + roleName + "/process"

  def configFileUrl(serviceName: String, roleName: String, file: String): String = {
    configFilesUrl(serviceName, roleName) + "/configFiles/" + file
  }

  def services: Seq[Service] = {
    val response: HttpResponse[String] = Http(serviceListUrl).auth("admin", "admin").asString
    val json = JacksonSupport.mapper.readTree(response.body)
    json.get("items").elements().asScala.map { node =>
      Service(node.get("name").textValue, node.get("type").textValue)
    }.toVector
  }

  def roles(serviceName: String): Seq[Role] = {
    val response: HttpResponse[String] = Http(rolesUrl(serviceName)).auth("admin", "admin").asString
    val json = JacksonSupport.mapper.readTree(response.body)
    json.get("items").elements().asScala.map { node =>
      Role(node.get("name").textValue, node.get("type").textValue, node.get("roleState").textValue)
    }.toVector
  }

  def configFiles(serviceName: String, roleName: String): Seq[ConfigFile] = {
    val response: HttpResponse[String] = Http(configFilesUrl(serviceName, roleName)).auth("admin", "admin").asString
    val json = JacksonSupport.mapper.readTree(response.body)
    json.get("configFiles").elements().asScala.map(_.textValue).map(ConfigFile.apply).toVector
  }

  def fetchFile(serviceName: String, roleName: String, file: String): String = {
    Http(configFileUrl(serviceName, roleName, file)).auth("admin", "admin").asString.body
  }

  println(services)

  // find hive service and get the service name for that
  val hive = services.find(_.`type` == "HIVE").get
  val r = roles(hive.name)

  println(r)

  // find role HIVEMETASTORE which will use the hive-site.xml
  val hiveMetastore = r.find(_.`type` == "HIVEMETASTORE").get
  println(hiveMetastore)

  val files = configFiles(hive.name, hiveMetastore.name)
  println(files)

  // find the hive-site.xml file and download it
  val file = files.find(_.name.contains("hive-site.xml")).get
  val contents = fetchFile(hive.name, hiveMetastore.name, file.name)
  println(contents)

  //configFileListUrl = / services / CD -HIVE - YdfvVqBU / roles / CD - HIVE - YdfvVqBU - HIVEMETASTORE - f06b8586b5e953b1b66d22dcf0dc2bf0 / process"
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
