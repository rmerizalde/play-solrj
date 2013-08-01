package org.apache.solr.client.solrj.request

import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.Future

import org.apache.solr.client.solrj.AsyncSolrServer
import org.apache.solr.client.solrj.response.UpdateResponse

class AsyncUpdateRequest extends UpdateRequest with AsyncRequest {

   // @todo figure out exception handling
  def process(server: AsyncSolrServer) : Future[UpdateResponse] = {
    val startTime: Long = System.currentTimeMillis()

    server.request(this).map { response =>
      val res = new UpdateResponse()
      res.setResponse(response)
      res.setElapsedTime(System.currentTimeMillis()-startTime)
      res
    }
  }
}
