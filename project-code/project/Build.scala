import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "play-solrj"
  val appVersion      = "0.1.1-SNAPSHOT"

  val appDependencies = Seq(
    "org.apache.solr" % "solr-solrj" % "4.4.0",
    "org.apache.solr" % "solr-test-framework" % "4.4.0" % "test",
    "org.mockito" % "mockito-all" % "1.9.5" % "test"
  )


  val main = play.Project(appName, appVersion, appDependencies).settings(
    scalacOptions ++= Seq("-feature"),
    resolvers += "Restlet repository" at "http://maven.restlet.org/",
    organization  := "org.opencommercesearch",
    publishMavenStyle := true,
    publishTo <<= (version) { version: String =>
       val scalasbt = "http://repo.scala-sbt.org/scalasbt/"
       val (name, url) = if (version.contains("-SNAPSHOT"))
         ("sbt-plugin-snapshots", scalasbt+"sbt-plugin-snapshots")
       else
         ("sbt-plugin-releases", scalasbt+"sbt-plugin-releases")
       Some(Resolver.url(name, new URL(url))(Resolver.mavenStylePatterns))
    }
  )

}
