package org.apache.solr.client.solrj.impl

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.Future
import scala.volatile
import scala.collection.convert.Wrappers.JIterableWrapper

import java.io.IOException
import java.util
import java.util.concurrent.TimeoutException
import java.net.URLDecoder

import org.apache.solr.client.solrj.{SolrServerException, SolrRequest, AsyncSolrServer}
import org.apache.solr.client.solrj.request.IsUpdateRequest
import org.apache.solr.common.cloud._
import org.apache.solr.common.SolrException
import org.apache.solr.common.SolrException.ErrorCode
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.util.{NamedList, StrUtils}
import org.apache.zookeeper.KeeperException
import org.apache.solr.client.solrj.util.ClientUtils

import AsyncLBHttpSolrServer._

object AsyncCloudSolrServer {
  def apply(zkHost: String) = new AsyncCloudSolrServer(zkHost, updatesToLeaders = true, AsyncLBHttpSolrServer())
}

/**
 * SolrJ client class to communicate with SolrCloud.
 * Instances of this class communicate with Zookeeper to discover Solr endpoints for SolrCloud collections, and then use the
 * {LBHttpSolrServer} to issue requests.
 *
 * @param zkHost The client endpoint of the zookeeper quorum containing the cloud state,
 * in the form HOST:PORT.
 */
class AsyncCloudSolrServer(zkHost: String, updatesToLeaders:Boolean, lbServer: AsyncLBHttpSolrServer) extends AsyncSolrServer {

  @volatile
  private[this] var _zkStateReader: ZkStateReader = null
  private[this] val rand = new util.Random()
  private[this] val cacheLock = new Object()
  @volatile
  private[this] var lastClusterStateHashCode = 0

  def zkStateReader = _zkStateReader
  // for tests
  private[impl] def zkStateReader_= (zkStateReader: ZkStateReader) { _zkStateReader = zkStateReader }

  @volatile
  var defaultCollection: String = null
  var zkConnectTimeout = 10000
  var zkClientTimeout = 10000

  // since the state shouldn't change often, should be very cheap reads
  val urlLists = new util.HashMap[String, util.List[String]]
  val leaderUrlLists = new util.HashMap[String, util.List[String]]
  val replicasLists = new util.HashMap[String, util.List[String]]

  /**
   * Connect to the zookeeper ensemble.
   * This is an optional method that may be used to force a connect before any other requests are sent.
   *
   */
   def connect() : Unit = {
    if (zkStateReader == null) {
      this.synchronized {
        if (zkStateReader == null) {
          try {
            val zk = new ZkStateReader(zkHost, zkConnectTimeout, zkClientTimeout)
            zk.createClusterStateWatchersAndUpdate()
            _zkStateReader = zk
          } catch {
            case e: InterruptedException => {
              Thread.currentThread().interrupt()
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
            }
            case e: KeeperException => {
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
            }
            case e: IOException => {
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
            }
            case e: TimeoutException => {
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e)
            }
          }
        }
      }
    }
  }

  override def request(request: SolrRequest) : Future[NamedList[Object]] = {
    connect()

    val theUrlList = buildUrlList(request)
    val req = new Req(request, theUrlList)
    lbServer.request(req).map({ rsp =>
      rsp.rsp
    })
  }

  private def buildUrlList(request: SolrRequest) : util.ArrayList[String] = {
    val clusterState = zkStateReader.getClusterState
    var reqParams = request.getParams

    if (reqParams == null) {
      reqParams = new ModifiableSolrParams
    }

    var theUrlList = new util.ArrayList[String]

    if (request.getPath.equals("/admin/collections") || request.getPath.equals("/admin/cores")) {
      val liveNodes = clusterState.getLiveNodes

      for (liveNode: String <- JIterableWrapper(liveNodes)) {
        val splitPointBetweenHostPortAndContext = liveNode.indexOf("_")
        theUrlList.add("http://"
          + liveNode.substring(0, splitPointBetweenHostPortAndContext) + "/"
          + URLDecoder.decode(liveNode, "UTF-8").substring(splitPointBetweenHostPortAndContext + 1))
      }
    } else {
      var collection = reqParams.get("collection", defaultCollection)

      if (collection == null) {
        throw new SolrServerException("No collection param specified on request and no default collection has been set.")
      }

      val collectionsList = getCollectionList(clusterState, collection)

      if (collectionsList.size() == 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection: " + collection)
      }
      collection = collectionsList.iterator().next()

      val collectionString = new StringBuilder()
      val it = collectionsList.iterator()

      for (i <- 0 to collectionsList.size() - 1) {
        val col = it.next()
        collectionString.append(col)
        if (i < collectionsList.size() - 1) {
          collectionString.append(",")
        }
      }
      // TODO: not a big deal because of the caching, but we could avoid looking
      // at every shard when getting leaders if we tweaked some things

      // Retrieve slices from the cloud state and, for each collection
      // specified, add it to the Map of slices.
      val slices = new util.HashMap[String, Slice]

      for (collectionName: String <- JIterableWrapper(collectionsList)) {
        val colSlices = clusterState.getActiveSlices(collectionName)
        if (colSlices == null) {
          throw new SolrServerException("Could not find collection:" + collectionName)
        }
        ClientUtils.addSlices(slices, collectionName, colSlices, true)
      }

      val liveNodes = clusterState.getLiveNodes

      cacheLock.synchronized {
        var leaderUrlList = leaderUrlLists.get(collection)
        var urlList = urlLists.get(collection)
        var replicasList = replicasLists.get(collection)

        var sendToLeaders = false
        var tmpReplicaList: util.List[String] = null

        if (request.isInstanceOf[IsUpdateRequest] && updatesToLeaders) {
          sendToLeaders = true
          tmpReplicaList = new util.ArrayList[String]
        }

        if ((sendToLeaders && leaderUrlList == null)
          || (!sendToLeaders && urlList == null)
          || clusterState.hashCode() != lastClusterStateHashCode) {
          // build a map of unique nodes
          val nodes = new util.HashMap[String, ZkNodeProps]
          val tmpUrlList = new util.ArrayList[String]
          for (slice: Slice <- JIterableWrapper(slices.values())) {
            for (nodeProps: ZkNodeProps <- JIterableWrapper(slice.getReplicasMap.values())) {
              val coreNodeProps = new ZkCoreNodeProps(nodeProps)
              val node = coreNodeProps.getNodeName

              if (liveNodes.contains(coreNodeProps.getNodeName)
                && coreNodeProps.getState.equals(ZkStateReader.ACTIVE)) {
                val seenNode = nodes.put(node, nodeProps) != null

                if (!seenNode) {
                  if (!sendToLeaders || (sendToLeaders && coreNodeProps.isLeader)) {
                    val url = coreNodeProps.getCoreUrl
                    tmpUrlList.add(url)
                  } else if (sendToLeaders) {
                    val url = coreNodeProps.getCoreUrl
                    tmpReplicaList.add(url)
                  }
                }
              }
            }
          }

          if (sendToLeaders) {
            leaderUrlLists.put(collection, tmpUrlList)
            leaderUrlList = tmpUrlList
            replicasLists.put(collection, tmpReplicaList)
            replicasList = tmpReplicaList
          } else {
            urlLists.put(collection, tmpUrlList)
            urlList = tmpUrlList
          }
          this.lastClusterStateHashCode = clusterState.hashCode()
        }

        if (sendToLeaders) {
          theUrlList = new util.ArrayList[String](leaderUrlList.size())
          theUrlList.addAll(leaderUrlList)
        } else {
          theUrlList = new util.ArrayList[String](urlList.size())
          theUrlList.addAll(urlList)
        }
        util.Collections.shuffle(theUrlList, rand)
        if (sendToLeaders) {
          val theReplicas = new util.ArrayList[String](replicasList.size())
          theReplicas.addAll(replicasList)
          util.Collections.shuffle(theReplicas, rand)
          theUrlList.addAll(theReplicas)
        }
      }
    }
    theUrlList
  }

  private def getCollectionList(clusterState: ClusterState, collection: String) : util.Set[String] = {
    // Extract each comma separated collection name and store in a List.
    val rawCollectionsList = StrUtils.splitSmart(collection, ",", true)
    val collectionsList = new util.HashSet[String]()
    // validate collections
    for (collectionName: String <- JIterableWrapper(rawCollectionsList)) {
      if (!clusterState.getCollections.contains(collectionName)) {
        val aliases = zkStateReader.getAliases
        val alias = aliases.getCollectionAlias(collectionName)
        if (alias != null) {
          val aliasList = StrUtils.splitSmart(alias, ",", true)
          collectionsList.addAll(aliasList)
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Collection not found: " + collectionName)
        }
      }
      collectionsList.add(collectionName)
    }
    collectionsList
  }

  override def shutdown() : Unit = {
    if (zkStateReader != null) {
      this.synchronized {
        if (zkStateReader!= null)
          zkStateReader.close()
        _zkStateReader = null
      }
    }

    if (lbServer != null) {
      lbServer.shutdown()
    }
  }

}
