package org.apache.solr.client.solrj.request

import play.api.libs.concurrent.Execution.Implicits._

import org.apache.solr.client.solrj.AsyncSolrServer
import scala.concurrent.Future
import org.apache.solr.client.solrj.response.SolrPingResponse

class AsyncSolrPing extends SolrPing with AsyncRequest {

  def process(server: AsyncSolrServer) : Future[SolrPingResponse] = {
    val startTime = System.currentTimeMillis()

    server.request(this).map { response =>
      // @todo passing null solr server to query response. QueryResponse use the server's
      // bean binder. Beans are not supported yet. WIll revisit latter
      val res = new SolrPingResponse()
      res.setResponse(response)
      res.setElapsedTime(System.currentTimeMillis() - startTime)
      res
    }
  }

}
