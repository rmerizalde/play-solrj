import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "lb-solr-server-sample"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    "play-solrj" % "play-solrj_2.10" % "0.1-SNAPSHOT"
  )


  val main = play.Project(appName, appVersion, appDependencies).settings(
  )

}
