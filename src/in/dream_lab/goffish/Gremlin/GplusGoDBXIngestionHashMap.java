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

public class GplusGoDBXIngestionHashMap { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  
  public static void main(String args[]) throws FileNotFoundException, IOException {
	 
	  GplusGoDBXIngestionHashMap godbIngest= new GplusGoDBXIngestionHashMap();
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
	
	  HashMap<String,TitanVertex> vertexMap = new HashMap<>();
	  long count=0;
	  //creates schema and indices for properties
//	  createSchema();
	  open();
	  long start=System.currentTimeMillis();
//	  //adding vertices
	  try (BufferedReader br = new BufferedReader(new FileReader(vertexPropFilePath))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       String[] data = line.split("\\W");
		       String sgid= data[0];
		       String rid = data[1];
		       String emp=data[2];
		       String school=data[3];
		       String major=data[4];
		       String places=data[5];
		       TitanVertex titanVertex = titanGraph.addVertex();
		       TitanVertexProperty<String> p1 = titanVertex.property("rid", rid);
		       TitanVertexProperty<String> p2 = titanVertex.property("emp", emp);
		       TitanVertexProperty<String> p3 = titanVertex.property("school", school);
		       TitanVertexProperty<String> p4 = titanVertex.property("major", major);
		       TitanVertexProperty<String> p5 = titanVertex.property("places", places);
//		       TitanVertexProperty<String> p3 = titanVertex.property("watch", watch);
		       vertexMap.put(rid, titanVertex);
		       count++;
		       
		       if(count%1000000==0) {//committing in batches
		    	   System.out.println("Vertices Ingested:" + count);
		   
		       }
		    }
		}catch(Exception e) {
			e.printStackTrace();
		}
	  
	  System.out.println("Loading Vertices Done.....Loading Edges");
	  System.out.println("Number of Vertices:" + count);
	  System.out.println("Time Vertices:" + (System.currentTimeMillis()-start));
	  start=System.currentTimeMillis();
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
			       TitanVertex v = vertexMap.get(rid);
			       
//			       System.out.println("Looking up sink:");
			       for(int i=3;i<3 + lCount;i++) {
			    	   String sinkVid=data[i];
			    	   TitanVertex s = vertexMap.get(sinkVid);
			    	   v.addEdge("d",s);

			       }
			       
			       
			       if(count%1000000==0) {
			    	   System.out.println("Edges Ingested:" + count);
			   
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
	  
	  System.out.println("Time Edges:" + (System.currentTimeMillis()-start));
	  
	  System.exit(1);
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
		System.exit(1);
}





public void printStats() {
	System.out.println("Vertices:"+titanGraph.traversal().V().count().next());
	System.out.println("Edges:"+titanGraph.traversal().E().count().next());
	System.exit(1);
}


public void printVertices() {
	GraphTraversal<Vertex, Vertex> k = titanGraph.traversal().V();
	while(k.hasNext()) {
		Vertex v = k.next();
		System.out.println("V:" + v.value("vid").toString() + "," + v.value("contr"));
	}
}






public GplusGoDBXIngestionHashMap(StandardTitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
  
  public GplusGoDBXIngestionHashMap() { 
//      titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open();
//    titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
	  titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","titan").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
  } 
   
 public void open() {
	 if(!titanGraph.isOpen())
//	 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
		 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","titan").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
 }
 
 public void close() {
	 if(!titanGraph.isClosed()) {
		 titanGraph.close();
	 }
 }
 
 public void sparkGraphConfigOpen() {
	 titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("ids.block-size","1000000").set("storage.buffer-size","100000").set("storage.batch-loading", true).set("storage.cassandra.keyspace","recreated").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30")
			 .set("gremlin.graph","org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph")
			 .set("gremlin.hadoop.graphInputFormat","com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraInputFormat")
			 .set("gremlin.hadoop.graphOutputFormat","org.apache.hadoop.mapreduce.lib.output.NullOutputFormat")
			 .set("gremlin.hadoop.jarsInDistributedCache",true)
			 .set("gremlin.hadoop.inputLocation","none")
			 .set("gremlin.hadoop.outputLocation","/user/abhilash/SparkGraphComputerOut")
			 .set("fs.defaultFS","hdfs://10.24.24.2:8020")
			 .set("titanmr.ioformat.conf.storage.backend","cassandrathrift")
			 .set("titanmr.ioformat.conf.storage.cassandra.keyspace","recreated")
			 .set("titanmr.ioformat.conf.storage.hostname","192.168.0.12,192.168.0.13,192.168.0.15,192.168.0.16,192.168.0.17,192.168.0.18,192.168.0.19,192.168.0.20,192.168.0.21,192.168.0.22,192.168.0.23,192.168.0.24,192.168.0.25,192.168.0.26,192.168.0.27")
			 .set("spark.master","spark://192.168.0.12:7077")
			 .set("spark.executor.memory","16g")
			 .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
			 .open();
 }


   
 }



