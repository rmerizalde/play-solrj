package controllers

import play.api.libs.concurrent.Execution.Implicits._

import play.api._
import play.api.mvc._
import play.api.libs.json._

import org.apache.solr.client.solrj.impl.AsyncHttpSolrServer
import org.apache.solr.client.solrj.{SolrServerException, SolrQuery}
import org.apache.solr.common.SolrDocument
import scala.collection.convert.Wrappers.JIterableWrapper

object Application extends Controller {
  
  def search(q: String) = Action {
    val server = AsyncHttpSolrServer("http://localhost:8983/solr")
    val query = new SolrQuery(q)

    Async {
      server.query(query).map(response => {
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