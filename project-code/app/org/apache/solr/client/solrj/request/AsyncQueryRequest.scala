package org.apache.solr.client.solrj.request

import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.{Future}

import org.apache.solr.common.params.SolrParams
import org.apache.solr.client.solrj.{AsyncSolrServer}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.SolrRequest.METHOD

class AsyncQueryRequest(params: SolrParams, method: METHOD) extends QueryRequest(params, method) with AsyncRequest {

  def this(params: SolrParams) = this(params, METHOD.GET)

   // @todo figure out exception handling
  def process(server: AsyncSolrServer) : Future[QueryResponse] = {
    val startTime = System.currentTimeMillis()

    server.request(this).map { response =>
      // @todo passing null solr server to query response. QueryResponse use the server's
      // bean binder. Beans are not supported yet. WIll revisit latter
      val res = new QueryResponse(response, null)
      res.setElapsedTime(System.currentTimeMillis() - startTime)
      res
    }
  }
}
