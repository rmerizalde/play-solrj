package org.apache.solr.client.solrj

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

import scala.concurrent.Future

import java.lang.Object
import java.{util => jutil}

import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.params.SolrParams
import org.apache.solr.common.util.NamedList
import org.apache.solr.client.solrj.request.{AsyncSolrPing, AsyncQueryRequest, AsyncUpdateRequest}
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION
import org.apache.solr.client.solrj.response.{SolrPingResponse, QueryResponse, UpdateResponse}
import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser

/**
 * @todo add support for beans??
 */
abstract class AsyncSolrServer {
  /**
   * Adds a collection of documents
   * @param docs  the collection of documents
   * @throws IOException If there is a low-level I/O error.
   */
  def add(docs: jutil.Collection[SolrInputDocument]) : Future[UpdateResponse] = {
    add(docs, -1)
  }

 /**
   * Adds a collection of documents, specifying max time before they become committed
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen
   * @throws IOException If there is a low-level I/O error.
   * @since solr 3.5
   */
  def add(docs: jutil.Collection[SolrInputDocument], commitWithinMs: Int) : Future[UpdateResponse] = {
    val req  = new AsyncUpdateRequest()
    req.add(docs)
    req.setCommitWithin(commitWithinMs)
    req.process(this)
  }

  /**
   * Adds the documents supplied by the given iterator.
   *
   * @param docIterator
   *          the iterator which returns SolrInputDocument instances
   *
   * @return the response from the SolrServer
   */
  def add(docIterator: jutil.Iterator[SolrInputDocument]) : Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest()
    req.setDocIterator(docIterator)
    req.process(this)
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * <p>
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * @throws IOException If there is a low-level I/O error.
   */
  def commit: Future[UpdateResponse] = {
    commit(waitFlush = true, waitSearcher = true)
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @throws IOException If there is a low-level I/O error.
   */
  def optimize: Future[UpdateResponse] = {
    optimize(waitFlush = true, waitSearcher = true, maxSegments = 1)
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
   * @throws IOException If there is a low-level I/O error.
   */
  def commit(waitFlush: Boolean, waitSearcher: Boolean): Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest()
    req.setAction(ACTION.COMMIT, waitFlush, waitSearcher)
    req.process(this)
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
   * @param softCommit makes index changes visible while neither fsync-ing index files nor writing a new index descriptor
   * @throws IOException If there is a low-level I/O error.
   */
  def commit(waitFlush: Boolean, waitSearcher: Boolean, softCommit: Boolean): Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest()
    req.setAction(ACTION.COMMIT, waitFlush, waitSearcher, softCommit)
    req.process(this)
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
   * @throws IOException If there is a low-level I/O error.
   */
  def optimize(waitFlush: Boolean, waitSearcher: Boolean): Future[UpdateResponse] = {
    optimize(waitFlush, waitSearcher, 1)
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
   * @param maxSegments  optimizes down to at most this number of segments
   * @throws IOException If there is a low-level I/O error.
   */
  def optimize(waitFlush: Boolean, waitSearcher: Boolean, maxSegments: Int): Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest()
    req.setAction(ACTION.OPTIMIZE, waitFlush, waitSearcher, maxSegments)
    req.process(this)
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   * @throws IOException If there is a low-level I/O error.
   */
  def rollback: Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest()
    req.rollback
    req.process(this)
  }

  /**
   * Deletes a single document by unique ID
   * @param id  the ID of the document to delete
   * @throws IOException If there is a low-level I/O error.
   */
  def deleteById(id: String): Future[UpdateResponse] = {
    deleteById(id, -1)
  }

  /**
   * Deletes a single document by unique ID, specifying max time before commit
   * @param id  the ID of the document to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   * @throws IOException If there is a low-level I/O error.
   * @since 3.6
   */
  def deleteById(id: String, commitWithinMs: Int): Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest
    req.deleteById(id)
    req.setCommitWithin(commitWithinMs)
    req.process(this)
  }

  /**
   * Deletes a list of documents by unique ID
   * @param ids  the list of document IDs to delete
   * @throws IOException If there is a low-level I/O error.
   */
  def deleteById(ids: jutil.List[String]): Future[UpdateResponse] = {
    deleteById(ids, -1)
  }

  /**
   * Deletes a list of documents by unique ID, specifying max time before commit
   * @param ids  the list of document IDs to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   * @throws IOException If there is a low-level I/O error.
   * @since 3.6
   */
  def deleteById(ids: jutil.List[String], commitWithinMs: Int): Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest
    req.deleteById(ids)
    req.setCommitWithin(commitWithinMs)
    req.process(this)
  }

  /**
   * Deletes documents from the index based on a query
   * @param query  the query expressing what documents to delete
   * @throws IOException If there is a low-level I/O error.
   */
  def deleteByQuery(query: String): Future[UpdateResponse] = {
    deleteByQuery(query, -1)
  }

  /**
   * Deletes documents from the index based on a query, specifying max time before commit
   * @param query  the query expressing what documents to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   * @throws IOException If there is a low-level I/O error.
   * @since 3.6
   */
  def deleteByQuery(query: String, commitWithinMs: Int): Future[UpdateResponse] = {
    val req = new AsyncUpdateRequest
    req.deleteByQuery(query)
    req.setCommitWithin(commitWithinMs)
    req.process(this)
  }

  /**
   * Issues a ping request to check if the server is alive
   * @throws IOException If there is a low-level I/O error.
   */
  def ping: Future[SolrPingResponse] = {
    new AsyncSolrPing().process(this)
  }

  /**
   * Performs a query to the Solr server
   * @param params  an object holding all key/value parameters to send along the request
   */
  def query(params: SolrParams) : Future[QueryResponse] = {
    new AsyncQueryRequest(params).process(this)
  }

  /**
   * Performs a query to the Solr server
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   */
  def query(params: SolrParams, method: METHOD) : Future[QueryResponse] = {
    new AsyncQueryRequest(params, method).process(this)
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will
   * send events for each Document rather then add them to the QueryResponse.
   *
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return
   * the results in the QueryResponse.
   *
   * @since solr 4.0
   */
  def queryAndStreamResponse(params: SolrParams, callback: StreamingResponseCallback) : Future[QueryResponse] = {
    val parser = new StreamingBinaryResponseParser(callback)
    val req = new AsyncQueryRequest(params)
    req.setStreamingResponseCallback(callback)
    req.setResponseParser(parser)
    req.process(this)
  }

  /**
   * SolrServer implementations need to implement how a request is actually processed
   */
  def request(request: SolrRequest) : Future[NamedList[Object]]

  /**
   * Release allocated resources.
   *
   * @since solr 4.0
   */
  def shutdown() : Unit
}
