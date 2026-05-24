ThisBuild / scalaVersion := "3.3.5"

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
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
  .dependsOn(smithyModule)

// Infrastructure Layer - This houses your Lambda handlers and AWS specifics
lazy val infra = (project in file("infra"))
  .settings(
    name := "whats-on-eire-infra",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-lambda-java-core" % "1.2.3",
      "com.amazonaws" % "aws-lambda-java-events" % "3.16.1"
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