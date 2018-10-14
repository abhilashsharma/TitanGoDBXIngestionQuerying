package in.dream_lab.goffish.Gremlin;

import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanEdge; 
import com.thinkaurelius.titan.core.TitanFactory; 
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.query.Query;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
import org.apache.cassandra.utils.OutputHandler.SystemOutput;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.BitSet;

public class GplusGoDBXRemoteEdgeIngestion { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  
  public static void main(String args[]) throws FileNotFoundException, IOException {
	 
	  GplusGoDBXRemoteEdgeIngestion godbIngest= new GplusGoDBXRemoteEdgeIngestion();
	  String vertexPropFilePath=args[0];
	  String edgePropFilePath=args[1];
	  String command=args[2];
	  switch(command) {
	  case "ingest":
		  godbIngest.IngestGraph(vertexPropFilePath,edgePropFilePath);
	  	  break;
	  case "clearGraph":
		  godbIngest.CleanGraph(); 
		  break;
	  case "printStats":
		  godbIngest.printStats();
		  break;
	  case "printVertices":
		  godbIngest.printVertices();
		  
	  }
	  
  } 
   
  
  


  private  void IngestGraph(String vertexPropFilePath, String edgePropFilePath) throws FileNotFoundException, IOException {
	
	  long count=0;
	  //creates schema and indices for properties
//	  createSchema();
	  open();

	  HashMap<String, Vertex> vertexMap = new HashMap<>();

	  //adding edges
	  try (BufferedReader br = new BufferedReader(new FileReader(edgePropFilePath))) {
		    String line;
		    count=0;
		    while ((line = br.readLine()) != null) {
		    	   String[] data= line.trim().split("\\W");
			       String sgid= data[0];
			       String rid = data[1];
			       int lCount= Integer.parseInt(data[2]);
//			       System.out.println("Looking up src:" + rid);
			       
			       Vertex v=vertexMap.get(rid);
			       if(v==null) {
			       v = titanGraph.traversal().V().has("rid", rid).next();
			       vertexMap.put(rid,v);
			       }
			       
//			       System.out.println("Looking up sink:");
			       for(int i=3 + lCount ;i<data.length;i++) {
			    	   String sinkVid=data[i];
			    	   Vertex s=vertexMap.get(sinkVid);
			    	   if(s==null) {
			    		   s = titanGraph.traversal().V().has("rid",sinkVid).next();
			    		   vertexMap.put(sinkVid, s);
			    	   }
			    	   
			    	   
			    	   v.addEdge("d",s);

			       }
			       
			       
			       if(count%1000000==0) {
			    	   System.out.println("Remote Edges Ingested:" + count);
//			    	   titanGraph.tx().commit();
			       }
			       count++;
			       
		    }
		}catch(Exception e) {
			e.printStackTrace();
			titanGraph.tx().rollback();
		}
	  
	  //committing the transaction
	  titanGraph.tx().commit();
	  
	  System.out.println("Loading Edges Done... Committed transaction");
	  
	  System.out.println("Checking Graph Edges:"+ count);
	  
	
}





private void CleanGraph() {
//System.out.println("Clearing Graph:" + titanGraph.traversal().V().count().next());
titanGraph.close();
TitanCleanup.clear(titanGraph);
titanGraph.tx().commit();

}





private void createSchema() {
// //This is for RGraph
//	TitanManagement mgmt = titanGraph.openManagement();
//	PropertyKey rid = mgmt.makePropertyKey("rid").dataType(String.class).make();
//	PropertyKey contr = mgmt.makePropertyKey("contr").dataType(String.class).make();
////	PropertyKey watch = mgmt.makePropertyKey("watch").dataType(String.class).make();
//	mgmt.buildIndex("rid",Vertex.class).addKey(rid).unique().buildCompositeIndex();
//	mgmt.buildIndex("contr",Vertex.class).addKey(contr).buildCompositeIndex();
////	mgmt.buildIndex("watch",Vertex.class).addKey(watch).buildCompositeIndex();
//	mgmt.makeEdgeLabel("d").make();
//	mgmt.commit();
	
	 //This is for gplus
	//"employer", "school"," major","places_lived"
		TitanManagement mgmt = titanGraph.openManagement();
		PropertyKey rid = mgmt.makePropertyKey("rid").dataType(String.class).make();
		PropertyKey emp = mgmt.makePropertyKey("emp").dataType(String.class).make();
		PropertyKey school = mgmt.makePropertyKey("school").dataType(String.class).make();
		PropertyKey major = mgmt.makePropertyKey("major").dataType(String.class).make();
		PropertyKey places = mgmt.makePropertyKey("places").dataType(String.class).make();
		mgmt.buildIndex("rid",Vertex.class).addKey(rid).unique().buildCompositeIndex();
		mgmt.buildIndex("emp",Vertex.class).addKey(emp).buildCompositeIndex();
		mgmt.buildIndex("school",Vertex.class).addKey(school).buildCompositeIndex();
		mgmt.buildIndex("major",Vertex.class).addKey(major).buildCompositeIndex();
		mgmt.buildIndex("places",Vertex.class).addKey(places).buildCompositeIndex();
		mgmt.makeEdgeLabel("d").multiplicity(Multiplicity.MULTI).make();
		mgmt.commit();

}





public void printStats() {
	System.out.println("Vertices:"+titanGraph.traversal().V().count().next());
	System.out.println("Edges:"+titanGraph.traversal().E().count().next());
}


public void printVertices() {
	GraphTraversal<Vertex, Vertex> k = titanGraph.traversal().V();
	while(k.hasNext()) {
		Vertex v = k.next();
		System.out.println("V:" + v.value("vid").toString() + "," + v.value("contr"));
	}
}







public GplusGoDBXRemoteEdgeIngestion(StandardTitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
  
  public GplusGoDBXRemoteEdgeIngestion() { 
//      titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open();
//    titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("query.batch",true).set("ids.block-size","500000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
	  titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","titan").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
  } 
   
 public void open() {
	 if(!titanGraph.isOpen())
//	 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("query.batch",true).set("ids.block-size","500000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
		 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","titan").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
 }
 
 public void close() {
	 if(!titanGraph.isClosed()) {
		 titanGraph.close();
	 }
 }


   
 }



