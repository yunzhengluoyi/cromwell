package cromwell.util

import java.io.{IOException, ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils

import scala.util.Try

object StringUtil {

  implicit class StringCompression(val str: String) extends AnyVal {
    def deflate: Try[String] = Try {
      val arrOutputStream = new ByteArrayOutputStream()
      val zipOutputStream = new GZIPOutputStream(arrOutputStream)
      zipOutputStream.write(str.getBytes)
      zipOutputStream.close()
      Base64.encodeBase64String(arrOutputStream.toByteArray)
    }

    def inflate: Try[String] = Try {
      IOUtils.toString(new GZIPInputStream(new ByteArrayInputStream(Base64.decodeBase64(str))))
    } recover {
      case e: IOException => str
    }

  }
}
