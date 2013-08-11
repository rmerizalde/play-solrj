
import org.apache.solr.client.solrj.impl.AsyncLBHttpSolrServer
import play.api._

package object globals {
  lazy val solrServer = AsyncLBHttpSolrServer("http://localhost:8983/solr", "http://localhost:8984/solr")
}

object Global extends GlobalSettings {

  override def onStart(app: Application) {

  }

  override def onStop(app: Application) {
    Logger.info("Shutting down Solr LB server")
    globals.solrServer.shutdown()
  }
}

