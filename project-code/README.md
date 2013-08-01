play-solrj
===============

The goal of this project is to implement a Solr non-blocking client for Play. Even though most of the code is just a port
from Solr's Java code, it hasn't been tested in production yet. My intention was to reuse the most Java code as possible
to limit the amount of code that needs to be rewritten. The client allows to do all key operations including document updates,
queries, commits, rollbacks, deletes, etc. The ConcurrentUpdateSolrServer is out the scope. The beans support is out of the
scope as well for this first version. There are other limitations as well due to Play's WS interface that will be
revisited later (e.g. response compression). Perhaps using Ning's AsyncHttpServer instead of Play's WS will allow to sort
them out.

SolrJ will include an non-blocking client using Java futures (See https://issues.apache.org/jira/browse/SOLR-3383). This
plugin is intended for Play Scala applications.

This is my first Scala project so there likely many things that could be done better. Please send any feedback.


