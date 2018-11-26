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
    "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
)
