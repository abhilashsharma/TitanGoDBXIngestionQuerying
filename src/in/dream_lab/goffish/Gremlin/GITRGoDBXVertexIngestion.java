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

public class GITRGoDBXVertexIngestion { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  
  public static void main(String args[]) throws FileNotFoundException, IOException {
	 
	  GITRGoDBXVertexIngestion godbIngest= new GITRGoDBXVertexIngestion();
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
		  break;
	  case "createSchema":
		  godbIngest.createSchema();
		  break;	  
	  }
	  
  } 
   
  
  


  private  void IngestGraph(String vertexPropFilePath, String edgePropFilePath) throws FileNotFoundException, IOException {
	

	  long count=0;
	  //creates schema and indices for properties
//	  createSchema();
	  open();
	  
//	  //adding vertices
	  try (BufferedReader br = new BufferedReader(new FileReader(vertexPropFilePath))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       String[] data = line.split("\\W");
		       String sgid= data[0];
		       long rid = Long.parseLong(data[1]);
		       String lang=data[2];
		       String ind=data[3];
		       String cont=data[4];
		       String ispub=data[5];
		       String follow=data[6];
		       TitanVertex titanVertex = titanGraph.addVertex();
		       TitanVertexProperty<Long> p1 = titanVertex.property("rid", rid);
		       TitanVertexProperty<String> p2 = titanVertex.property("language", lang);
		       TitanVertexProperty<String> p3 = titanVertex.property("industry", ind);
		       TitanVertexProperty<String> p4 = titanVertex.property("contr", cont);
		       TitanVertexProperty<String> p5 = titanVertex.property("ispublic", ispub);
		       TitanVertexProperty<String> p6 = titanVertex.property("follow", follow);
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

	  //committing the transaction
	  titanGraph.tx().commit();
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
		PropertyKey lang = mgmt.makePropertyKey("language").dataType(String.class).make();
		PropertyKey ind = mgmt.makePropertyKey("industry").dataType(String.class).make();
		PropertyKey cont = mgmt.makePropertyKey("contr").dataType(String.class).make();
		PropertyKey ispub = mgmt.makePropertyKey("ispublic").dataType(String.class).make();
		PropertyKey follow = mgmt.makePropertyKey("follow").dataType(String.class).make();
		mgmt.buildIndex("rid",Vertex.class).addKey(rid).unique().buildCompositeIndex();
		mgmt.buildIndex("emp",Vertex.class).addKey(lang).buildCompositeIndex();
		mgmt.buildIndex("school",Vertex.class).addKey(ind).buildCompositeIndex();
		mgmt.buildIndex("major",Vertex.class).addKey(cont).buildCompositeIndex();
		mgmt.buildIndex("places",Vertex.class).addKey(ispub).buildCompositeIndex();
		mgmt.buildIndex("places",Vertex.class).addKey(follow).buildCompositeIndex();
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
		System.out.println("V:" + v.value("rid").toString() + "," + v.value("contr"));
	}
}







public GITRGoDBXVertexIngestion(StandardTitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
  
  public GITRGoDBXVertexIngestion() { 
//      titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open();
    titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","500000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").open();
  } 
   
 public void open() {
	 if(!titanGraph.isOpen())
		 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","500000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").open();
 }
 
 public void close() {
	 if(!titanGraph.isClosed()) {
		 titanGraph.close();
	 }
 }


   
 }



