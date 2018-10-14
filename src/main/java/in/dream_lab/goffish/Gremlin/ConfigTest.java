package in.dream_lab.goffish.Gremlin;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;


public class ConfigTest {

  public static void main(String args[])
  {

//      Vertex rash = titanGraph.addVertex(null);
	  StandardTitanGraph titanGraph = (StandardTitanGraph) TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","titan").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.23,192.168.0.24,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
	  titanGraph.isOpen();
  }
}
