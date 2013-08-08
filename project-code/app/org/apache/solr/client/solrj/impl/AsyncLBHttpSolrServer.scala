package org.apache.solr.client.solrj.impl

/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import play.api.libs.concurrent.Execution.Implicits._
import play.api.Play.current
import play.api.libs.concurrent.Akka

import scala.collection.convert.Wrappers.JIterableWrapper
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.volatile

import akka.actor.Cancellable

import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util
import java.io.IOException

import org.apache.solr.client.solrj._
import org.apache.solr.common.util.NamedList
import org.apache.solr.common.SolrException

import AsyncLBHttpSolrServer._

object AsyncLBHttpSolrServer {
  class ServerWrapper(val solrServer: AsyncHttpSolrServer) {
    var lastUsed: Long = 0    // last time used for a real request
    var lastChecked: Long = 0 // last time checked for liveness

    // "standard" servers are used by default.  They normally live in the alive list
    // and move to the zombie list when unavailable.  When they become available again,
    // they move back to the alive list.
    var standard:Boolean = true

    var failedPings: Int = 0

    val key: String = solrServer.baseUrl

    override def toString : String = {
      solrServer.baseUrl
    }

    override def hashCode : Int = {
      key.hashCode
    }

    override def equals(obj: Any) : Boolean = {
      super.equals(obj)
      if (this == obj) true
      else if (!obj.isInstanceOf[ServerWrapper]) false
      else this.key.equals(obj.asInstanceOf[ServerWrapper].key)
    }
  }

  class Req(val request: SolrRequest, val servers: List[String]) {

    /**
     * the number of dead servers to try if there are no live servers left
     * Defaults to the number of servers in this request
     */
    var numDeadServersToTry: Int = servers.length
  }

  class Rsp {
    // The server that returned the response
    var server: String = null
    // The response from the server
    var rsp: NamedList[Object] = null
  }

  def normalize(server: String) : String = {
    if (server.endsWith("/")) server.substring(0, server.length() - 1)
    else server
  }

  def apply(solrServerUrl: String*) = new AsyncLBHttpSolrServer(new BinaryResponseParser(), solrServerUrl:_*)

  val solrQuery = new SolrQuery("*:*").setRows(0)
  val CheckInterval = 60 * 1000 //1 minute between checks
  val NonStandardPingLimit = 5  // number of times we'll ping dead servers not in the server list
}

/**
 * LBHttpSolrServer or "LoadBalanced HttpSolrServer" is a load balancing wrapper around
 * {org.apache.solr.client.solrj.impl.HttpSolrServer}. This is useful when you
 * have multiple SolrServers and the requests need to be Load Balanced among them.
 *
 * Do <b>NOT</b> use this class for indexing in master/slave scenarios since documents must be sent to the
 * correct master; no inter-node routing is done.
 *
 * In SolrCloud (leader/replica) scenarios, this class may be used for updates since updates will be forwarded
 * to the appropriate leader.
 *
 * Also see the <a href="http://wiki.apache.org/solr/LBHttpSolrServer">wiki</a> page.
 *
 * <p/>
 * It offers automatic failover when a server goes down and it detects when the server comes back up.
 * <p/>
 * Load balancing is done using a simple round-robin on the list of servers.
 * <p/>
 * If a request to a server fails by an IOException due to a connection timeout or read timeout then the host is taken
 * off the list of live servers and moved to a 'dead server list' and the request is resent to the next live server.
 * This process is continued till it tries all the live servers. If at least one server is alive, the request succeeds,
 * and if not it fails.
 * <blockquote><pre>
 * SolrServer lbHttpSolrServer = new LBHttpSolrServer("http://host1:8080/solr/","http://host2:8080/solr","http://host2:8080/solr");
 * //or if you wish to pass the HttpClient do as follows
 * httpClient httpClient =  new HttpClient();
 * SolrServer lbHttpSolrServer = new LBHttpSolrServer(httpClient,"http://host1:8080/solr/","http://host2:8080/solr","http://host2:8080/solr");
 * </pre></blockquote>
 * This detects if a dead server comes alive automatically. The check is done in fixed intervals in a dedicated thread.
 * This interval can be set using {#setAliveCheckInterval} , the default is set to one minute.
 * <p/>
 * <b>When to use this?</b><br/> This can be used as a software load balancer when you do not wish to setup an external
 * load balancer. Alternatives to this code are to use
 * a dedicated hardware load balancer or using Apache httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @since solr 1.4
 */
class AsyncLBHttpSolrServer(private[this] val parser: ResponseParser, solrServerUrl: String*) extends AsyncSolrServer {

  // keys to the maps are currently of the form "http://localhost:8983/solr"
  // which should be equivalent to CommonsHttpSolrServer.getBaseURL()
  private[this] val aliveServers:util.Map[String, ServerWrapper] = new util.LinkedHashMap[String, ServerWrapper]
  // access to aliveServers should be synchronized on itself

  private[this] val zombieServers: util.Map[String, ServerWrapper]  = new ConcurrentHashMap[String, ServerWrapper]

  // changes to aliveServers are reflected in this array, no need to synchronize
  @volatile
  private[this] var aliveServerList = new Array[ServerWrapper](0)

  private[this] val counter = new AtomicInteger(-1)
  private[this] var interval = CheckInterval
  private[impl] var aliveCheckActor: Cancellable = null

  def aliveServerCount = aliveServerList.length
  def zombieServerCount = zombieServers.size()

  for (s: String <- solrServerUrl) {
    val wrapper = new ServerWrapper(makeServer(s))
    aliveServers.put(wrapper.key, wrapper)
  }
  updateAliveList()

  protected def makeServer(server: String) : AsyncHttpSolrServer = {
    new AsyncHttpSolrServer(server, parser)
  }

  /**
   * Tries to query a live server. A SolrServerException is thrown if all servers are dead.
   * If the request failed due to IOException then the live server is moved to dead pool and the request is
   * retried on another live server.  After live servers are exhausted, any servers previously marked as dead
   * will be tried before failing the request.
   *
   * @param request the SolrRequest.
   *
   * @return response
   *
   * @throws IOException If there is a low-level I/O error.
   */
  override def request(request: SolrRequest) : Future[NamedList[Object]] = {
    val serverList: Array[ServerWrapper] = aliveServerList

    withAliveServers(request, serverList, attempts = 0, justFailedServers = null, ex = null)
  }

  private def withAliveServers(request: SolrRequest, serverList: Array[ServerWrapper], attempts: Int,
      justFailedServers: util.Map[String,ServerWrapper], ex: Exception) : Future[NamedList[Object]] = {
    val maxTries = serverList.length

    if (attempts < maxTries) {
      val count = counter.incrementAndGet()
      val wrapper = serverList(count % serverList.length)

      wrapper.lastUsed = System.currentTimeMillis()
      wrapper.solrServer.request(request).map({ response =>
        response
      }).recoverWith {
        case e: SolrException => Future.failed(e) // Server is alive but the request was malformed or invalid
        case e: SolrServerException => {
          if (e.getRootCause.isInstanceOf[IOException]) {
            moveAliveToDead(wrapper)
            var justFailed = justFailedServers
            if (justFailed == null) justFailed = new util.HashMap[String,ServerWrapper]
            justFailed.put(wrapper.key, wrapper)
            withAliveServers(request, serverList, attempts + 1, justFailed, e)
          } else {
            Future.failed(e)
          }
        }
        case other => Future.failed(new SolrServerException(other))
      }
    } else {
      withZombieServers(request, zombieServers.values().iterator(), justFailedServers, ex)
    }
  }

  private def withZombieServers(request: SolrRequest, zombieServersIt: util.Iterator[ServerWrapper],
        justFailedServers: util.Map[String,ServerWrapper], ex: Exception) : Future[NamedList[Object]] = {
    if (zombieServersIt.hasNext) {
      val wrapper = zombieServersIt.next

      if (wrapper.standard && justFailedServers != null && !justFailedServers.containsKey(wrapper.key)) {
        wrapper.solrServer.request(request).map({ response =>
          // remove from zombie list *before* adding to alive to avoid a race that could lose a server
          zombieServers.remove(wrapper.key)
          addToAlive(wrapper)
          response
        }).recoverWith {
          case e: SolrException => Future.failed(e) // Server is alive but the request was malformed or invalid
            case e: SolrServerException => {
            if (e.getRootCause.isInstanceOf[IOException]) {
              // still dead
              withZombieServers(request, zombieServersIt, justFailedServers, e)
            } else {
              Future.failed(e)
            }
          }
          case other => Future.failed(new SolrServerException(other))
        }
      } else {
        withZombieServers(request, zombieServersIt, justFailedServers, ex)
      }
    } else {
      if (ex == null) {
        throw new SolrServerException("No live SolrServers available to handle this request")
      } else {
        throw new SolrServerException("No live SolrServers available to handle this request", ex)
      }
    }
  }

  /*private def addZombie(server: AsyncHttpSolrServer, e: Exception) : Exception = {
    val wrapper = new ServerWrapper(server)
    wrapper.lastUsed = System.currentTimeMillis()
    wrapper.standard = false
    zombieServers.put(wrapper.key, wrapper)
    startAliveCheckExecutor()
    e
  }*/

  private def updateAliveList() : Unit = {
    aliveServers.synchronized  {
      aliveServerList = aliveServers.values().toArray(new Array[ServerWrapper](aliveServers.size()))
    }
  }

  private def removeFromAlive(key: String) : ServerWrapper = {
    aliveServers.synchronized {
      val wrapper = aliveServers.remove(key)
      if (wrapper != null)
        updateAliveList()
      wrapper
    }
  }

  private def addToAlive(wrapper: ServerWrapper) : Unit = {
    aliveServers.synchronized {
      aliveServers.put(wrapper.key, wrapper)
      // TODO: warn if there was a previous entry?
      updateAliveList()
    }
  }

  def addSolrServer(server: String) : Unit = {
    val solrServer = makeServer(server)
    addToAlive(new ServerWrapper(solrServer))
  }

  private[impl] def addSolrServer(solrServer: AsyncHttpSolrServer) : Unit = {
    addToAlive(new ServerWrapper(solrServer))
  }

  def removeSolrServer(server: String) : Unit = {
    var s = new URL(server).toExternalForm
    if (s endsWith "/") {
      s = server.take(server.length() - 1)
    }

    // there is a small race condition here - if the server is in the process of being moved between
    // lists, we could fail to remove it.
    removeFromAlive(s)
    zombieServers.remove(s)
  }

  override def shutdown() : Unit = {
    if (aliveCheckActor != null) {
      aliveCheckActor.cancel()
    }
  }

  def checkZombieServers() : Unit = {
    for (zombieServer: ServerWrapper <- JIterableWrapper(zombieServers.values())) {
      checkAZombieServer(zombieServer)
    }
  }

  /**
    * Takes up one dead server and check for aliveness. The check is done in a roundrobin. Each server is checked for
    * aliveness once in 'x' millis where x is decided by the setAliveCheckinterval() or it is defaulted to 1 minute
    *
    * @param zombieServer a server in the dead pool
    */
  def checkAZombieServer(zombieServer: ServerWrapper) : Unit = {
    val currTime = System.currentTimeMillis()
    zombieServer.lastChecked = currTime
    val resp = zombieServer.solrServer.query(solrQuery)

    resp.map( response => {
      if (response.getStatus == 0) {
        // server has come back up.
        // make sure to remove from zombies before adding to alive to avoid a race condition
        // where another thread could mark it down, move it back to zombie, and then we delete
        // from zombie and lose it forever.
        val wrapper = zombieServers.remove(zombieServer.key)
        if (wrapper != null) {
          wrapper.failedPings = 0
          if (wrapper.standard) {
            addToAlive(wrapper)
          }
        } else {
          // something else already moved the server from zombie to alive
        }
      }
    }).recover {
      case e: Exception => {
        //Expected. The server is still down.
        zombieServer.failedPings += 1

        // If the server doesn't belong in the standard set belonging to this load balancer
        // then simply drop it after a certain number of failed pings.
        if (!zombieServer.standard && zombieServer.failedPings >= NonStandardPingLimit) {
          zombieServers.remove(zombieServer.key)
        }
      }
    }
  }

  private def moveAliveToDead(wrapper: ServerWrapper) : Unit = {
    val aliveWrapper = removeFromAlive(wrapper.key)

    // another thread already detected the failure and removed it
    if (aliveWrapper == null) return

    zombieServers.put(aliveWrapper.key, wrapper)
    startAliveCheckExecutor()
  }

  protected override def finalize() : Unit = {
    try {
      if(aliveCheckActor != null)
        aliveCheckActor.cancel()
    } finally {
      super.finalize()
    }
  }

  /**
   * AsyncLBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use this to set that
   * interval
   *
   * @param interval time in milliseconds
   */
  def aliveCheckInterval_=(interval: Int) = {
    if (interval <= 0) {
      throw new IllegalArgumentException("Alive check interval must be " +
              "positive, specified value = " + interval)
    }
    this.interval = interval
  }

  def aliveCheckInterval = this.interval

  private[impl] def startAliveCheckExecutor() : Unit = {
    // double-checked locking, but it's OK because we don't *do* anything with aliveCheckExecutor
    // if it's not null.
    if (aliveCheckActor == null) {
      this.synchronized {
        if (aliveCheckActor == null) {
          // use default's Akka actory system
          aliveCheckActor = Akka.system.scheduler.schedule(interval.milliseconds, interval.milliseconds) {
            checkZombieServers()
          }
        }
      }
    }
  }
}
