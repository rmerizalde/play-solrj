package org.apache.solr.client.solrj.impl

import play.api.test.WithApplication

import scala.concurrent.Future

import java.util

import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import org.mockito.ArgumentCaptor
import org.apache.solr.client.solrj.SolrRequest
import org.apache.solr.common.cloud.{Replica, Slice, ClusterState, ZkStateReader}

import AsyncLBHttpSolrServer._
import scala.collection.convert.Wrappers.JIterableWrapper
import org.apache.solr.client.solrj.request.UpdateRequest

/**
 * Test the AsyncLBHttpSolrServer.
 */
class AsyncCloudSolrServerSpec extends Specification with Mockito {

  val server1 = "127.0.0.01:8900"
  val server2 = "127.0.0.01:8901"
  val server3 = "127.0.0.01:8902"
  val urlServer1 = s"http://$server1/solr"
  val urlServer2 = s"http://$server2/solr"
  val urlServer3 = s"http://$server3/solr"
  val testCollection = "myCollection"

  "The AsyncCloudSolrServer" should {

    def setupServers = {
      val lbServer = mock[AsyncLBHttpSolrServer]
      val server = spy(new AsyncCloudSolrServer("", updatesToLeaders = true, lbServer))
      val zkStateReader = setupZkStateReader
      val response = mock[Rsp]

      server.zkStateReader = zkStateReader
      lbServer.request(any[Req]) returns Future.successful(response)
      server.defaultCollection = testCollection

      (server, lbServer)
    }

    def setupZkStateReader = {
      val zkStateReader = mock[ZkStateReader]
      val clusterState = mock[ClusterState]

      zkStateReader.getClusterState returns clusterState

      // live nodes
      val liveNodes = new util.HashSet[String](3)
      liveNodes.add(s"${server1}_solr")
      liveNodes.add(s"${server2}_solr")
      liveNodes.add(s"${server3}_solr")
      clusterState.getLiveNodes returns liveNodes

      // slices
      val slices = new util.ArrayList[Slice](1)
      val testSlice = mock[Slice]

      slices.add(testSlice)
      clusterState.getActiveSlices(testCollection) returns slices

      // slice replicas

      val replicaMap = new util.HashMap[String, Replica](3)
      replicaMap.put(server1 + "_solr", setupReplica(server1, isLeader = false))
      replicaMap.put(server2 + "_solr", setupReplica(server2, isLeader = true))
      replicaMap.put(server3 + "_solr", setupReplica(server3, isLeader = false))
      testSlice.getReplicasMap returns replicaMap

      // collections
      val collections = new util.HashSet[String](1)
      collections.add(testCollection)
      clusterState.getCollections returns collections

      zkStateReader
    }

    def setupReplica(server: String, isLeader: Boolean) : Replica = {
      val replica = mock[Replica]
      replica.getName returns server + "_myCollection"
      replica.getStr(ZkStateReader.NODE_NAME_PROP) returns server + "_solr"
      replica.getStr(ZkStateReader.STATE_PROP) returns ZkStateReader.ACTIVE
      replica.getStr(ZkStateReader.BASE_URL_PROP) returns s"http://$server/solr"
      replica.getStr(ZkStateReader.CORE_NAME_PROP) returns testCollection
      if (isLeader) {
        replica.containsKey(ZkStateReader.LEADER_PROP) returns true
      }
      replica
    }

    "try to use all live nodes when executing an admin request" in new WithApplication {
      val (server, lbServer) = setupServers
      val request = mock[SolrRequest]
      val argument = ArgumentCaptor.forClass(classOf[Req])

      request.getPath returns "/admin/collections"
      server.request(request)

      try {
        there was one(lbServer).request(argument.capture())
        val serverUrls = JIterableWrapper(argument.getValue.servers)

        serverUrls must containAllOf(Seq(urlServer1, urlServer2, urlServer3))
      } finally {
        server.shutdown()
      }
    }

    "add all replica urls in the alive nodes" in new WithApplication {
      val (server, lbServer) = setupServers
      val request = mock[SolrRequest]
      val argument = ArgumentCaptor.forClass(classOf[Req])

      request.getPath returns "/select"
      server.request(request)

      try {
        there was one(lbServer).request(argument.capture())
        val serverUrls = JIterableWrapper(argument.getValue.servers)

        serverUrls must containAllOf(Seq(
          s"$urlServer1/$testCollection/",
          s"$urlServer2/$testCollection/",
          s"$urlServer3/$testCollection/"))
      } finally {
        server.shutdown()
      }
    }

    "add the leaders at the front of the url list when sending updates to leaders" in new WithApplication {
      val (server, lbServer) = setupServers
      val request = mock[UpdateRequest]
      val argument = ArgumentCaptor.forClass(classOf[Req])

      request.getPath returns "/select"
      server.request(request)

      try {
        there was one(lbServer).request(argument.capture())
        val serverUrls = JIterableWrapper(argument.getValue.servers)

        serverUrls must containAllOf(Seq(
          s"$urlServer1/$testCollection/",
          s"$urlServer2/$testCollection/",
          s"$urlServer3/$testCollection/"))
        argument.getValue.servers.get(0) must beEqualTo(s"$urlServer2/$testCollection/")
      } finally {
        server.shutdown()
      }
    }
  }
}
