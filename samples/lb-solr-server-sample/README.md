To run this sample you need to start Solr first.

See the README.txt in the example directory of the default Solr installation.

To run the example Solr Server:

  java -jar start.jar

in the example directory, and when Solr is started connect to

  http://localhost:8983/solr/

To add documents to the index, use the post.jar (or post.sh script) in
the example/exampledocs subdirectory (while Solr is running), for example:

     cd exampledocs
     java -jar post.jar *.xml
Or:  sh post.sh *.xml

For the sake of this sample app (ignoring replication), copy the example directory to example2 after populating the index.
In the example2 directory start the second server

java -jar start.jar -Djetty.port=8984
