package io.eels.yarn

object YarnUtils {
  def jarForClass(klass: Class[_]): String = {
    val uri = klass.getResource("/" + klass.getName.replace('.', '/') + ".class")
    assert(uri != null, s"Class $klass not found in resource path")

    // if the class is inside a jar, the uri will be of the form jar:file:/path/myjar.jar!/com.package/myclass.class
    if (uri.toString.startsWith("jar:file:")) {
      uri.toString.stripPrefix("jar:file:").split('!').head
    } else {
      sys.error(s"Class is not located in a jar [$uri]")
    }
  }
}
