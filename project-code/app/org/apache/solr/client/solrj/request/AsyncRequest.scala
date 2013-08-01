package org.apache.solr.client.solrj.request

import scala.concurrent.Future

import org.apache.solr.client.solrj.{SolrResponse, AsyncSolrServer}

trait AsyncRequest {

  def process(server: AsyncSolrServer) : Future[SolrResponse]

}
