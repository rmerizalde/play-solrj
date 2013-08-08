import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "play-solr"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    "org.apache.solr" % "solr-solrj" % "4.4.0",
    "org.apache.solr" % "solr-test-framework" % "4.4.0" % "test",
    "org.mockito" % "mockito-all" % "1.9.5" % "test"
  )


  val main = play.Project(appName, appVersion, appDependencies).settings(
    scalacOptions ++= Seq("-feature"),
    resolvers += "Restlet repository" at "http://maven.restlet.org/"
  )

}
