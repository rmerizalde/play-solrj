package org.apache.solr.client.solrj.request

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
