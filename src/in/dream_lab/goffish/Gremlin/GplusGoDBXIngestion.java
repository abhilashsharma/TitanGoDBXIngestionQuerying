package in.dream_lab.goffish.Gremlin;

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
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
import org.apache.cassandra.utils.OutputHandler.SystemOutput;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.BitSet;

public class GplusGoDBXIngestion { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  
  public static void main(String args[]) throws FileNotFoundException, IOException {
	 
	  GplusGoDBXIngestion godbIngest= new GplusGoDBXIngestion();
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
	  createSchema();
	  open();
	  
//	  //adding vertices
	  try (BufferedReader br = new BufferedReader(new FileReader(vertexPropFilePath))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       String[] data = line.split("\\W");
		       String sgid= data[0];
		       long rid = Long.parseLong(data[1]);
		       String emp=data[2];
		       String school=data[3];
		       String major=data[4];
		       String places=data[5];
		       TitanVertex titanVertex = titanGraph.addVertex();
		       TitanVertexProperty<Long> p1 = titanVertex.property("rid", rid);
		       TitanVertexProperty<String> p2 = titanVertex.property("emp", emp);
		       TitanVertexProperty<String> p3 = titanVertex.property("school", school);
		       TitanVertexProperty<String> p4 = titanVertex.property("major", major);
		       TitanVertexProperty<String> p5 = titanVertex.property("places", places);
//		       TitanVertexProperty<String> p3 = titanVertex.property("watch", watch);
		       count++;
		       
		       if(count%1000000==0) {//committing in batches
		    	   System.out.println("Vertices Ingested:" + count);
		    	   titanGraph.tx().commit();
		       }
		    }
		}catch(Exception e) {
			e.printStackTrace();
		}
	  
	  System.out.println("Loading Vertices Done.....Loading Edges");
	  System.out.println("Number of Vertices:" + count);
	  //adding edges
	  try (BufferedReader br = new BufferedReader(new FileReader(edgePropFilePath))) {
		    String line;
		    count=0;
		    while ((line = br.readLine()) != null) {
		    	   String[] data= line.trim().split("\\W");
			       String sgid= data[0];
			       String rid = data[1];
			       String lCount= data[2];
//			       System.out.println("Looking up src:" + rid);
			       Vertex v = titanGraph.traversal().V().has("rid", rid).next();
			       
//			       System.out.println("Looking up sink:");
			       for(int i=3;i<data.length;i++) {
			    	   String sinkVid=data[i];
			    	   Vertex s = titanGraph.traversal().V().has("rid",sinkVid).next();
			    	   v.addEdge("d",s ,null);

			       }
			       
			       
			       if(count%1000000==0) {
			    	   System.out.println("Edges Ingested:" + count);
			    	   titanGraph.tx().commit();
			       }
			       count++;
			       
		    }
		}catch(Exception e) {
			e.printStackTrace();
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
		mgmt.makeEdgeLabel("d").make();
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







public GplusGoDBXIngestion(StandardTitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
  
  public GplusGoDBXIngestion() { 
//      titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open();
    titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","500000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
  } 
   
 public void open() {
	 if(!titanGraph.isOpen())
	 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","500000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
 }
 
 public void close() {
	 if(!titanGraph.isClosed()) {
		 titanGraph.close();
	 }
 }


   
 }



