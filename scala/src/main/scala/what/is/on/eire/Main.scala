package what.is.on.eire

import cats.effect.IO
import cats.effect.IOApp
import java.nio.file.Files
import java.nio.file.Paths
import org.http4s.Uri
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import smithy4s.http4s.SimpleRestJsonBuilder

object Main extends IOApp.Simple {

  private val ticketmasterBaseUri: Uri =
    uri"https://app.ticketmaster.com"

  private val apiKey: String =
    readDotEnvValue("http/.env", "TICKETMASTER_API_KEY")

  private def readDotEnvValue(path: String, key: String): String = {
    val envPath = Paths.get(path)
    if (!Files.exists(envPath))
      throw new RuntimeException(s"Missing $path file")

    Files
      .readAllLines(envPath)
      .toArray
      .toList
      .collectFirst {
        case line: String if line.trim.startsWith(s"$key=") =>
          line.substring(line.indexOf("=") + 1).trim
      }
      .filter(_.nonEmpty)
      .getOrElse(throw new RuntimeException(s"Missing $key value in $path"))
  }

  override def run: IO[Unit] =
    (for {
      httpClient      <- EmberClientBuilder.default[IO].build
      ticketmasterApi <- SimpleRestJsonBuilder(TicketmasterApi)
                           .client(httpClient)
                           .uri(ticketmasterBaseUri)
                           .resource
      routes          <- SimpleRestJsonBuilder
                           .routes(
                             new MainService[IO](
                               new TicketmasterClient[IO](ticketmasterApi, apiKey)
                             )
                           )
                           .resource
      server          <- EmberServerBuilder
                           .default[IO]
                           .withHttpApp(routes.orNotFound)
                           .build
    } yield server).useForever
}
