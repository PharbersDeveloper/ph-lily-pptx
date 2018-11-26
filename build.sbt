import play.routes.compiler.InjectedRoutesGenerator
import play.sbt.PlayScala
import scala.language.experimental.macros

def common = Seq(
    scalaVersion := "2.11.8",
    version := "0.1",
    organization := "com.pharbers"
)

lazy val root = (project in file(".")).
    enablePlugins(PlayScala).
    settings(common: _*).
    settings(
        name := "ph-lily-pptx",
        fork in run := true,
        javaOptions += "-Xmx2G"
    )

routesGenerator := InjectedRoutesGenerator

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
    guice,
    "com.pharbers" % "pharbers-module" % "0.1",

    "org.apache.spark" % "spark-core_2.11" % "2.3.0",
    "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
    "org.apache.spark" % "spark-yarn_2.11" % "2.3.0",
    "org.apache.hadoop" % "hadoop-common" % "2.7.2",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2",

    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7" force(),

    "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
)
