ThisBuild / scalaVersion := "3.3.5"

lazy val root = (project in file("."))
  .aggregate(scalaModule)

lazy val scalaModule = (project in file("scala"))
  .enablePlugins(Smithy4sCodegenPlugin)
  .settings(
    name := "whats-on-eire",
    libraryDependencies ++= Seq(
      "com.disneystreaming.smithy4s" %% "smithy4s-core" % smithy4sVersion.value
    )
  )