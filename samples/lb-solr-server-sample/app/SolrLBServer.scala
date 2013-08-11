import org.apache.solr.client.solrj.impl.{BinaryResponseParser, AsyncLBHttpSolrServer}

package object SolrLBServer extends AsyncLBHttpSolrServer(new BinaryResponseParser) {

}
