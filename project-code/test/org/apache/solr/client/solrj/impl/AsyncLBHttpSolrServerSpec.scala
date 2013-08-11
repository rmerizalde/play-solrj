package org.apache.solr.client.solrj.impl

import play.api.test.WithApplication
import akka.actor.Cancellable
import org.apache.solr.client.solrj.response.QueryResponse

import scala.concurrent.Future.successful
import scala.concurrent.Future.failed

import java.util
import java.io.IOException

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import org.apache.solr.client.solrj.{SolrServerException, SolrRequest, SolrQuery}
import org.apache.solr.common.util.NamedList
import org.apache.solr.common.params.SolrParams
import org.apache.solr.common.SolrException

import AsyncLBHttpSolrServer._

/**
 * Test the AsyncLBHttpSolrServer.
 */
class AsyncLBHttpSolrServerSpec extends Specification with Mockito {

  val urlServer1 = "http://foo.org:8900/solr"
  val urlServer2 = "http://foo.org:8901/solr"
  val parser = new BinaryResponseParser

  private[this] def setup(addHttpServers: Boolean) = {
    val lbServer = createServer()
    val server1 = mock[AsyncHttpSolrServer]
    val server2 = mock[AsyncHttpSolrServer]
    val response = mock[NamedList[Object]]
    val query = new SolrQuery("*:*")

    server1.baseUrl returns urlServer1
    server2.baseUrl returns urlServer2

    lbServer.makeServer(urlServer1) returns server1
    lbServer.makeServer(urlServer2) returns server2

    if (addHttpServers) {
      lbServer.addSolrServer(urlServer1)
      lbServer.addSolrServer(urlServer2)
    }

    (lbServer, server1, server2, response, query)
  }

  private[this] def createServer() : AsyncLBHttpSolrServer = {
    val server = spy(new AsyncLBHttpSolrServer(parser))

    // prevent alive check thread from starting
    server.aliveCheckActor = mock[Cancellable]
    server
  }

  "The AsyncLBHttpSolrServer" should {

    "mark failed server as zombie and try another alive server" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = true)
      val failedResponse = failed(new SolrServerException(new IOException()))

      server1.request(any[SolrRequest]) returns failedResponse
      server1.query(any[SolrParams]) returns failedResponse
      server2.request(any[SolrRequest]) returns successful(response)

      val futureResponse = lbServer.query(query)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was one(server2).request(any[SolrRequest])

        lbServer.aliveServerCount must equalTo(1)
        lbServer.zombieServerCount must equalTo(1)
        futureResponse.value.get.isSuccess must beTrue
      } finally {
        lbServer.shutdown()
      }
    }

    "mark all failed servers as zombies and throw an exception" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = true)
      val failedResponse = failed(new SolrServerException(new IOException()))

      server1.request(any[SolrRequest]) returns failedResponse
      server1.query(any[SolrParams]) returns failedResponse
      server2.request(any[SolrRequest]) returns failedResponse
      server2.query(any[SolrParams]) returns failedResponse

      val futureResponse = lbServer.query(query)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was one(server2).request(any[SolrRequest])
        there was one(server2).baseUrl

        lbServer.aliveServerCount must equalTo(0)
        lbServer.zombieServerCount must equalTo(2)
        futureResponse.value.get.isFailure must beTrue
      } finally {
        lbServer.shutdown()
      }
    }

    "mark zombie servers that recovered from failure as alive" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = true)
      val failedResponse = failed(new SolrServerException(new IOException()))

      server1.request(any[SolrRequest]) returns failedResponse
      server1.query(any[SolrParams]) returns failedResponse
      server2.request(any[SolrRequest]) returns failedResponse
      server2.query(any[SolrParams]) returns failedResponse

      val futureResponse = lbServer.query(query)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was one(server2).request(any[SolrRequest])
        there was one(server2).baseUrl

        lbServer.aliveServerCount must equalTo(0)
        lbServer.zombieServerCount must equalTo(2)
        futureResponse.value.get.isFailure must beTrue
      } finally {
        // nothing
      }

      val queryResponse = mock[QueryResponse]
      server1.query(any[SolrParams]) returns successful(queryResponse)
      server2.query(any[SolrParams]) returns successful(queryResponse)

      lbServer.checkZombieServers()

      try {
        lbServer.aliveServerCount must equalTo(2)
        lbServer.zombieServerCount must equalTo(0)
      } finally {
        lbServer.shutdown()
      }
    }

    "not mark server as zombie when a query error occurs" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = true)
      val exception = mock[SolrException]
      val failedResponse = failed(exception)

      server1.request(any[SolrRequest]) returns failedResponse
      server2.request(any[SolrRequest]) returns successful(response)

      val futureResponse = lbServer.query(query)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was no(server2).request(any[SolrRequest])

        lbServer.aliveServerCount must equalTo(2)
        lbServer.zombieServerCount must equalTo(0)
        futureResponse.value.get.isFailure must beTrue
      } finally {
        lbServer.shutdown()
      }
    }

    "throw an exception if all servers are zombies" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = true)
      val ioException = mock[IOException]
      val exception = mock[SolrServerException]
      val failedResponse = failed(exception)

      exception.getRootCause returns ioException
      server1.request(any[SolrRequest]) returns failedResponse
      server2.request(any[SolrRequest]) returns failedResponse

      val futureResponse = lbServer.query(query)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was one(server2).request(any[SolrRequest])
        there was one(server2).baseUrl

        lbServer.aliveServerCount must equalTo(0)
        lbServer.zombieServerCount must equalTo(2)
        futureResponse.value.get.isFailure must beTrue
        futureResponse.value.get.failed.map({ ex =>
          ex.getMessage must startWith("No live SolrServers available to handle this request")
        })
      } finally {
        lbServer.shutdown()
      }
    }

    /**
     * cloud server request tests
     */

    "skip non-standard zombie servers" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = false)
      val solrRequest = mock[SolrRequest]
      val serverList = new util.ArrayList[String](2)
      val exception = mock[SolrException]
      val failedResponse = failed(exception)

      server1.request(any[SolrRequest]) returns failedResponse
      server2.request(any[SolrRequest]) returns successful(response)
      lbServer.addZombie(server1)
      serverList.add(urlServer1)
      serverList.add(urlServer2)

      val req = new Req(solrRequest, serverList)
      val futureResponse = lbServer.request(req)

      try {
        there was no(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was one(server2).request(any[SolrRequest])

        lbServer.aliveServerCount must equalTo(0)
        lbServer.zombieServerCount must equalTo(1)
        futureResponse.value.get.isSuccess must beTrue
      } finally {
        lbServer.shutdown()
      }
    }

    "recover skipped non-standard zombie servers" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = false)
      val solrRequest = mock[SolrRequest]
      val serverList = new util.ArrayList[String](2)
      val exception = mock[SolrException]
      val failedResponse = failed(exception)

      server1.request(any[SolrRequest]) returns successful(response)
      server2.request(any[SolrRequest]) returns successful(response)
      lbServer.addZombie(server1)
      lbServer.addZombie(server2)
      serverList.add(urlServer1)
      serverList.add(urlServer2)

      val req = new Req(solrRequest, serverList)
      val futureResponse = lbServer.request(req)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was no(server2).request(any[SolrRequest])
        there was one(server2).baseUrl

        lbServer.aliveServerCount must equalTo(0)
        lbServer.zombieServerCount must equalTo(1)
        futureResponse.value.get.isSuccess must beTrue
      } finally {
        lbServer.shutdown()
      }
    }

    "throw an exception if all request servers are zombies" in new WithApplication {
      val (lbServer, server1, server2, response, query) = setup(addHttpServers = false)
      val solrRequest = mock[SolrRequest]
      val serverList = new util.ArrayList[String](2)
      val exception = mock[IOException]
      val failedResponse = failed(exception)

      server1.request(any[SolrRequest]) returns failedResponse
      server2.request(any[SolrRequest]) returns failedResponse
      lbServer.addZombie(server1)
      lbServer.addZombie(server2)
      serverList.add(urlServer1)
      serverList.add(urlServer2)

      val req = new Req(solrRequest, serverList)
      val futureResponse = lbServer.request(req)

      try {
        there was one(server1).request(any[SolrRequest])
        there was one(server1).baseUrl
        there was one(server2).request(any[SolrRequest])
        there was one(server2).baseUrl

        lbServer.aliveServerCount must equalTo(0)
        lbServer.zombieServerCount must equalTo(2)
        futureResponse.value.get.isFailure must beTrue
        futureResponse.value.get.isFailure must beTrue
        futureResponse.value.get.failed.map({ ex =>
          ex.getMessage must startWith("No live SolrServers available to handle this request")
        })
      } finally {
        lbServer.shutdown()
      }
    }
  }
}
