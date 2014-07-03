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
import play.api.libs.ws.WS
import play.api.libs.ws.Response

import scala.concurrent.Future
import scala.collection.convert.Wrappers.JIterableWrapper

import java.io.{ByteArrayOutputStream, IOException, InputStream}
import java.lang.{Throwable, String}
import java.nio.charset.Charset
import java.net.{SocketTimeoutException, ConnectException}
import java.util

import org.apache.solr.client.solrj.{SolrServerException, SolrRequest, AsyncSolrServer, ResponseParser}
import org.apache.solr.common.util.{ContentStream, NamedList}
import org.apache.solr.common.params.{CommonParams, ModifiableSolrParams}
import org.apache.solr.common.SolrException
import org.apache.solr.client.solrj.util.ClientUtils
import org.apache.http.{Header, HttpEntity, HttpStatus, NameValuePair}
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntity, FormBodyPart}
import org.apache.http.entity.mime.content.{InputStreamBody, StringBody}
import org.apache.http.message.{BasicHeader, BasicNameValuePair}
import org.apache.http.entity.{InputStreamEntity, ContentType}
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.solr.client.solrj.request.RequestWriter

import play.api.Play

object AsyncHttpSolrServer {
  /**
   * @param baseUrl
   *          The URL of the Solr server. For example, "
   *          <code>http://localhost:8983/solr/</code>" if you are using the
   *          standard distribution Solr webapp on your local machine.
   */
  def apply(baseUrl: String) = new AsyncHttpSolrServer(baseUrl, new BinaryResponseParser)
}

/**
 * This class implements functionality to communicate with Solr asynchronously via HTTP.
 *
 * The URL of the Solr server must be provided. By default, this class uses the BinaryResponseParser.
 * This parser represents the default Response Parser chosen to parse the
 * response if the parser were not specified as part of the req.
 *
 * @todo use ning's AsyncHttpClient directly? How about Apache's HttpAsyncClient for Java implementation?
 *
 * @constructor create new asynchronous HTTP Solr server
 * @param _baseUrl the URL of the Solr server
 * @param parser the parser used to parse responses from Solr
 *
 */
class AsyncHttpSolrServer(_baseUrl: String, var parser: ResponseParser) extends AsyncSolrServer {

  /**
   * User-Agent String.
   */
  val Agent = "Solr[" + classOf[AsyncHttpSolrServer].getName + "] 1.0"

  private val Utf_8 = "UTF-8"
  private val DefaultPath = "/select"

  // @todo revisit the need for invariantParams
  private[this] val _invariantParams : ModifiableSolrParams = null

  val baseUrl: String =
    if (_baseUrl.endsWith("/")) _baseUrl.substring(0, _baseUrl.length - 1)
    else _baseUrl

  if (baseUrl contains '?') throw new RuntimeException("Invalid base url for solrj.  The base URL must not contain parameters: " + baseUrl)


  var requestWriter: RequestWriter = new RequestWriter
  // @todo implement retries
  //private[this] var maxRetries = 0
  var useMultiPartPost = false
  var followRedirects = false
  var timeout = Play.current.configuration.getInt("ws.requestTimeout").getOrElse(0).toInt

  /**
   * Process the req. If
   * {org.apache.solr.client.solrj.SolrRequest#getResponseParser} is
   * null, then use {#getParser}
   *
   * @param req
   *          The {@see org.apache.solr.client.solrj.SolrRequest} to process
   * @return The {@see org.apache.solr.common.util.NamedList} result
   * @throws IOException If there is a low-level I/O error.
   *
   * @see #req(org.apache.solr.client.solrj.SolrRequest,
   *      org.apache.solr.client.solrj.ResponseParser)
   * @todo need to use ws.timeout.connection, ws.timeout.idle for timeout settings
   */
  override def request(req: SolrRequest) : Future[NamedList[Object]] = {
    var responseParser = req.getResponseParser
    if (responseParser == null) {
      responseParser = parser
    }
    request(req, responseParser)
  }

  def request(req: SolrRequest, processor: ResponseParser) : Future[NamedList[Object]] = {
    var params = req.getParams
    val streams = requestWriter.getContentStreams(req)
    var path = requestWriter.getPath(req)

    if (path == null || !path.startsWith("/")) {
      path = DefaultPath
    }

    var parser = req.getResponseParser
    if (parser == null) {
      parser = this.parser
    }

    // The parser 'wt=' and 'version=' params are used instead of the original
    // params
    val wparams = new ModifiableSolrParams(params)
    if (parser != null) {
      wparams.set(CommonParams.WT, parser.getWriterType)
      wparams.set(CommonParams.VERSION, parser.getVersion)
    }
    if (_invariantParams != null) {
      wparams.add(_invariantParams)
    }
    params = wparams

    try {
      if (SolrRequest.METHOD.GET == req.getMethod) {
        if( streams != null ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!" )
        }
        withResponse(WS.url(baseUrl + path + ClientUtils.toQueryString(params, false))
          .withFollowRedirects(followRedirects)
          .withHeaders(("User-Agent", Agent))
          .withRequestTimeout(timeout)
          .get(), processor)
      } else if (SolrRequest.METHOD.POST == req.getMethod ) {
        val url = baseUrl + path
        val isMultipart = streams != null && streams.size > 1
        val postParams = new util.LinkedList[NameValuePair]

        if (streams == null || isMultipart) {
          var requestHolder = WS.url(url).withHeaders(("Content-Charset", "UTF-8"))

          if (!this.useMultiPartPost && !isMultipart) {
            requestHolder = requestHolder.withHeaders(("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8"))
          }

          val parts = new util.LinkedList[FormBodyPart]
          val it = params.getParameterNamesIterator
          while (it.hasNext) {
            val p = it.next
            val values = params.getParams(p)
            if (values != null) {
              for (v:String <- values) {
                if (this.useMultiPartPost || isMultipart) {
                  parts.add(new FormBodyPart(p, new StringBody(v, Charset.forName("UTF-8"))))
                } else {
                  postParams.add(new BasicNameValuePair(p, v))
                }
              }
            }
          }

          if (isMultipart) {
            for (content:ContentStream <- new JIterableWrapper(streams)) {
              var contentType = content.getContentType
              if (contentType == null) {
                contentType = "application/octet-stream" // default
              }
              parts.add(new FormBodyPart(content.getName,
                   new InputStreamBody(
                       content.getStream,
                       contentType,
                       content.getName)))
            }
          }

          var entity:HttpEntity = null

          if (parts.size > 0) {
            val multipartEntity = new MultipartEntity(HttpMultipartMode.STRICT)
            entity = multipartEntity
            for(p:FormBodyPart <- new JIterableWrapper(parts)) {
              multipartEntity.addPart(p)
            }
          } else {
            //not using multipart
            entity = new UrlEncodedFormEntity(postParams, "UTF-8")
          }

          val outputStream = new ByteArrayOutputStream
          entity.writeTo(outputStream)
          withResponse(requestHolder
            .withFollowRedirects(followRedirects)
            .withHeaders(("User-Agent", Agent))
            .withRequestTimeout(timeout)
            .post(outputStream.toByteArray), processor)
        } else {
          // Single stream as body
          val contentStream:Array[ContentStream] = new Array(1)
          val it = streams.iterator

          if (it.hasNext) {
            contentStream(0) = it.next
          }

          val entity  = new InputStreamEntity(contentStream(0).getStream, -1) {
            override def getContentType : Header = {
              new BasicHeader("Content-Type", contentStream(0).getContentType)
            }

            @Override
            override def isRepeatable : Boolean = {
              false
            }
          }

          val outputStream = new ByteArrayOutputStream

          entity.writeTo(outputStream)
          // It is has one stream, it is the post body, put the params in the URL
          withResponse(WS.url(url + ClientUtils.toQueryString(params, false))
            .withFollowRedirects(followRedirects)
            .withHeaders(("User-Agent", Agent), ("Content-Type", contentStream(0).getContentType))
            .withRequestTimeout(timeout)
            .post(outputStream.toByteArray), processor)
        }
      }
      else {
        throw new SolrServerException("Unsupported method: " + req.getMethod )
      }
    } catch {
      case ex: IOException => throw new SolrServerException("error reading streams", ex)
    }
  }

  private def withResponse(wsResponse: Future[Response], processor: ResponseParser) : Future[NamedList[Object]] = {
    wsResponse.map { response =>
      var shouldClose:Boolean = true
      var respBody:InputStream = null

      try {
        val httpStatus = response.status

        // Read the contents
        respBody = response.ahcResponse.getResponseBodyAsStream

        // handle some http level checks before trying to parse the response
        httpStatus match  {
          case HttpStatus.SC_OK | HttpStatus.SC_BAD_REQUEST | HttpStatus.SC_CONFLICT =>
          case HttpStatus.SC_MOVED_PERMANENTLY | HttpStatus.SC_MOVED_TEMPORARILY =>
            if (!followRedirects) {
              throw new SolrServerException("Server at " + baseUrl + " sent back a redirect (" + httpStatus + ").")
            }
          case _ =>
            throw new SolrException(SolrException.ErrorCode.getErrorCode(httpStatus), "Server at " + baseUrl
                + " returned non ok status:" + httpStatus + ", message:" + response.statusText)
        }

        if (processor == null) {
          // no processor specified, return raw stream
          val rsp = new NamedList[Object]
          rsp.add("stream", respBody)
          // Only case where stream should not be closed
          shouldClose = false
          rsp
        } else {
          val charset = getContentCharset(response.ahcResponse.getContentType)
          val rsp = processor.processResponse(respBody, charset)
          if (httpStatus != HttpStatus.SC_OK) {
            var reason:String = null
            try {
              val err = rsp.get("error").asInstanceOf[NamedList[Object]]
              if (err != null) {
                reason = err.get("msg").asInstanceOf[String]
              }
            } catch {
              // TODO? get the trace?
              case ex: Exception =>
            }

            if (reason == null) {
              val msg = new StringBuilder
              msg.append(response.statusText)
              msg.append("\n\n")
              msg.append("req: " + response.ahcResponse.getUri)
              reason = java.net.URLDecoder.decode(msg.toString(), Utf_8)
            }
            throw new SolrException(
                SolrException.ErrorCode.getErrorCode(httpStatus), reason)
          }
          rsp
        }
      } catch {
        case e: ConnectException => throw new SolrServerException("Server refused connection at: " + baseUrl, e)
        case e: SocketTimeoutException => throw new SolrServerException("Timeout occured while waiting response from server at: " + baseUrl, e)
        case e: IOException => throw new SolrServerException("IOException occured when talking to server at: " + baseUrl, e)
      } finally {
        if (respBody != null && shouldClose) {
          try {
            respBody.close()
          } catch {
            case t: Throwable => // ignore
          }
        }
      }
    }
  }

  private def getContentCharset(contentType: String) : String = {
    var charsetName:String = null
    if (contentType != null) {
      val charset = ContentType.parse(contentType).getCharset
      if (charset != null) {
        charsetName = charset.name
      }
    }
    charsetName
  }

  override def shutdown() : Unit = {}
}
