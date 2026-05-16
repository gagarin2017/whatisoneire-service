addSbtPlugin("com.disneystreaming.smithy4s" % "smithy4s-sbt-codegen" % "0.18.49")
// Provides the 'assembly' task to merge our Scala 3 application, 
// Smithy4s, and AWS SDKs into a single self-contained "Fat JAR" for AWS Lambda.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")