
import org.apache.solr.client.solrj.impl.{AsyncCloudSolrServer, AsyncLBHttpSolrServer}
import play.api._

package object globals {
  lazy val solrServer = AsyncCloudSolrServer("localhost:9983")
}

object Global extends GlobalSettings {

  override def onStart(app: Application) {
    Logger.info("Initializing Solr cloud server")
    globals.solrServer.defaultCollection = "collection1"
  }

  override def onStop(app: Application) {
    Logger.info("Shutting down Solr cloud server")
    globals.solrServer.shutdown()
  }
}