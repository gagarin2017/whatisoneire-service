package what.is.on.eire

import java.nio.file.Files
import java.nio.file.Paths
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Reads config values from `http/.env` (local dev) or falls back to `System.getenv` (AWS Lambda).
  */
private[eire] object EnvLoader {

  private val envFilePath = Paths.get("http/.env")

  /** Returns the value for `key`, checking the `.env` file first, then the process environment. */
  def get(key: String): Option[String] =
    dotEnvValue(key).orElse(Option(System.getenv(key)))

  /** Same as `get`, but throws with a descriptive message if the key is missing. */
  def require(key: String): String =
    get(key).getOrElse(
      throw new RuntimeException(
        s"Missing required config: $key (not found in http/.env nor in environment variables)"
      )
    )

  /** Reads a single key from `http/.env` if the file exists. */
  private def dotEnvValue(key: String): Option[String] =
    Try {
      val lines = Files.readAllLines(envFilePath).asScala
      lines
        .collectFirst {
          case line if line.trim.startsWith(s"$key=") =>
            line.substring(line.indexOf("=") + 1).trim
        }
        .filter(_.nonEmpty)
    }.toOption.flatten
}
