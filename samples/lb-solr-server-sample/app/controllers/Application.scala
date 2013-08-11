package controllers

import play.api.libs.concurrent.Execution.Implicits._

import play.api.mvc._
import org.apache.solr.client.solrj.impl.{AsyncLBHttpSolrServer, AsyncHttpSolrServer}
import org.apache.solr.client.solrj.SolrQuery
import play.api.libs.json.{Json, JsArray}
import org.apache.solr.common.SolrDocument
import scala.collection.convert.Wrappers.JIterableWrapper
import org.apache.solr.client.solrj.SolrServerException

object Application extends Controller {
  
  def search(q: String) = Action {
    val query = new SolrQuery(q)

    Async {
      globals.solrServer.query(query).map(response => {
        val documents = response.getResults
        var results = new JsArray()

        for (doc: SolrDocument <- JIterableWrapper(documents)) {
          val jsonDoc = Json.obj(
            "id" -> doc.get("id").asInstanceOf[String],
            "name" -> doc.get("name").asInstanceOf[String],
            "price" -> doc.get("price").asInstanceOf[Float]
          )
          results = results.append(jsonDoc)
        }

        Ok(Json.obj(
          "found" -> documents.getNumFound,
          "results" -> results
        ))
      }).recover {
        case e: SolrServerException => {
          e.getRootCause.printStackTrace()
          if (e.getRootCause != null) {

            InternalServerError(e.getRootCause.getMessage)
          } else {
            InternalServerError(e.getMessage)
          }
        }
        case other: Exception => InternalServerError(other.getMessage)
      }
    }
  }
  
}