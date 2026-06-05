ThisBuild / scalaVersion := "3.3.5"

// Enable the plugin for all sub‑projects
ThisBuild / scalafmtOnCompile := true                   // optional: auto‑format on compile
ThisBuild / scalafmtConfig    := file(".scalafmt.conf") // path to your config file

// If you want the `formatAll` aggregate task:
ThisBuild / aggregate := true

lazy val smithyModule = (project in file("smithy"))
  .enablePlugins(Smithy4sCodegenPlugin)
  .settings(
    name := "whats-on-eire-smithy",
    libraryDependencies ++= Seq(
      "com.disneystreaming.smithy4s" %% "smithy4s-core" % smithy4sVersion.value
    )
  )

// Pure Application Core - NO AWS libraries allowed here!
lazy val scalaModule = (project in file("scala"))
  .settings(
    name := "whats-on-eire-app",
    libraryDependencies ++= Seq(
      "org.typelevel"                %% "cats-effect"         % "3.5.7",
      "com.disneystreaming"          %% "weaver-cats"         % "0.8.4",
      "com.disneystreaming.smithy4s" %% "smithy4s-http4s"     % smithy4sVersion.value,
      "com.disneystreaming.smithy4s" %% "smithy4s-json"       % smithy4sVersion.value,
      "org.http4s"                   %% "http4s-client"       % "0.23.27",
      "org.http4s"                   %% "http4s-ember-server" % "0.23.27",
      "org.http4s"                   %% "http4s-ember-client" % "0.23.27",
      "org.http4s"                   %% "http4s-dsl"          % "0.23.27" % Test
    )
  )
  .dependsOn(smithyModule)

// Infrastructure Layer - This houses your Lambda handlers and AWS specifics
lazy val infra = (project in file("infra"))
  .settings(
    name                             := "whats-on-eire-infra",
    libraryDependencies ++= Seq(
      "com.amazonaws"          % "aws-lambda-java-core"   % "1.2.3",
      "com.amazonaws"          % "aws-lambda-java-events" % "3.16.1",
      "software.amazon.awssdk" % "kinesis"                % "2.29.0"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    }
  )
  .dependsOn(scalaModule)

lazy val root = (project in file("."))
  .aggregate(smithyModule, scalaModule, infra)
  .settings(
    // Now, running or packaging at the root targets the infra module's artifact!
    Compile / run := (infra / Compile / run).evaluated
  )
