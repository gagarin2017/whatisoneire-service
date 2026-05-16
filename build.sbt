ThisBuild / scalaVersion := "3.3.5"

// 1. Smithy Module: Purely for Smithy definitions and generated case classes
lazy val smithyModule = (project in file("smithy"))
  .enablePlugins(Smithy4sCodegenPlugin)
  .settings(
    name := "whats-on-eire-smithy",
    libraryDependencies ++= Seq(
      "com.disneystreaming.smithy4s" %% "smithy4s-core" % smithy4sVersion.value
    )
  )

// 2. Scala Module: Your core application layer containing Main.scala
lazy val scalaModule = (project in file("scala"))
  .settings(
    name := "whats-on-eire-app"
  )
  .dependsOn(smithyModule)

// 3. Infra Module: For cloud adapters, AWS Lambda layers, or DB configurations
lazy val infra = (project in file("infra"))
  .settings(
    name := "whats-on-eire-infra"
  )
  .dependsOn(scalaModule)

// 4. Root Orchestrator: Routes 'sbt run' down to your application layer
lazy val root = (project in file("."))
  .aggregate(smithyModule, scalaModule, infra)
  .settings(
    Compile / run := (scalaModule / Compile / run).evaluated
  )