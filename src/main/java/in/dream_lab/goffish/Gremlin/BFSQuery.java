package in.dream_lab.goffish.Gremlin;
import com.thinkaurelius.titan.core.TitanEdge; 
import com.thinkaurelius.titan.core.TitanFactory; 
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.tinkerpop.blueprints.Direction; 
import com.tinkerpop.blueprints.Edge; 
import com.tinkerpop.blueprints.Vertex; 
import com.tinkerpop.gremlin.java.GremlinPipeline; 
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle; 
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
import org.apache.cassandra.utils.OutputHandler.SystemOutput;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tools.ant.taskdefs.Exit;
import java.util.BitSet;

public class BFSQuery { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  
  public static void main(String args[]) { 
//   GraphDatabase graph = new TitanGraphDatabase(); 
//   graph.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH); 
//   graph.massiveModeLoading("./data/youtubeEdges.txt"); 
//   graph.shutdownMassiveGraph(); 
   BFSQuery titanQuery = new BFSQuery();
   if(args[1].equals("int")){
   try
   {
   String FILENAME=args[0];
   FileReader fr = new FileReader(FILENAME);
   BufferedReader br = new BufferedReader(fr);

   String sCurrentLine;

   br = new BufferedReader(new FileReader(FILENAME));

   while ((sCurrentLine = br.readLine()) != null) {
           System.out.println(sCurrentLine);
           String[] strs=sCurrentLine.trim().split(",");
           String key=strs[0].replaceAll("^\"|\"$", "");;
           Object val=Integer.parseInt(strs[1]);
           int depth= Integer.parseInt(strs[2]);
           titanQuery.BfsMultiQuery(key,val,depth);
   }
   
   }catch(Exception e){
     
   }
   }
   else{//assuming it is string
     try
     {
     String FILENAME=args[0];
     FileReader fr = new FileReader(FILENAME);
     BufferedReader br = new BufferedReader(fr);

     String sCurrentLine;

     br = new BufferedReader(new FileReader(FILENAME));

     while ((sCurrentLine = br.readLine()) != null) {
             System.out.println(sCurrentLine);
             String[] strs=sCurrentLine.trim().split(",");
             String key=strs[0].replaceAll("^\"|\"$", "");;
             Object val=strs[1];
             int depth= Integer.parseInt(strs[2]);
             titanQuery.BfsMultiQuery(key,val,depth);
     }
     
     }catch(Exception e){
       
     }     
   }
   
   
   
   //Testing and Debugging
//   titanQuery.BfsMultiQuery("patid",4564956,3);
//   titanQuery.TestBfsQuery("patid",4564956,3);
   
  
  } 
   
  
  


  public BFSQuery(StandardTitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
  
  public BFSQuery() { 
//      titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open();
    titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","titan").set("index.search.backend","elasticsearch").set("storage.hostname","192.168.0.23,192.168.0.24,192.168.0.26,192.168.0.27").set("index.search.hostname","192.168.0.27,192.168.0.28,192.168.0.29,192.168.0.30").open();
//    conf = new BaseConfiguration();
//    conf.setProperty("storage.backend","cassandra");
//    conf.setProperty("storage.cassandra.keyspace","recreated");
//    conf.setProperty("storage.connection-timeout","5000000");
//    conf.setProperty("storage.setup-wait","2000000");
//    conf.setProperty("index.search.backend","elasticsearch");
//    conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
//    conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
//    conf.setProperty("cache.db-cache","true");
//    titanGraph =  TitanFactory.open(conf); 
  } 
   
 
  private void TestBfsQuery(String key,Object val,final int depth) {
    final HashSet<Object> visitedSet=new HashSet<>();
    // TODO Auto-generated method stub
    System.out.println("In Test Query");
    
    PipeFunction<LoopBundle<Vertex>,Boolean> whileFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
        Object rootVertex=bundle.getPath().get(0);
        Object currentVertex=bundle.getPath().get(bundle.getPath().size()-1);
        System.out.println("checking Path:" + bundle.getPath().toString() + "," +((Vertex)currentVertex).getId());
       return (bundle.getLoops()< depth); 
      }
    };
    PipeFunction<LoopBundle<Vertex>,Boolean> emitFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){
      
      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
       return  true; 
      }
    };
    
    PipeFunction<Vertex,Boolean> filterFunction = new PipeFunction<Vertex,Boolean>(){

      @Override
      public Boolean compute(Vertex v) {
        System.out.println("returning " + !visitedSet.contains(v) );
       return !visitedSet.contains(v); 
      }
    };
    long t1= System.currentTimeMillis();
    
//    GremlinPipeline pipe = new GremlinPipeline(titanGraph).V(key,val).store(visitedSet).as("x").out().filter(filterFunction).store(visitedSet).loop("x", whileFunction,emitFunction ).path();
    //non-lazy evaluation
    List bfsPathList = new GremlinPipeline(titanGraph).V(key,val).store(visitedSet).as("x").out().filter(filterFunction).store(visitedSet).loop("x", whileFunction,emitFunction ).path().toList();
    
    System.out.println("Time: " + (System.currentTimeMillis()-t1));
    
    System.out.println("BFS Path Count:" + bfsPathList.size());
    for(Object o : bfsPathList){
      System.out.println("Path:" + o.toString());
    }
    System.out.println("Exiting querying");

  }

  private void BfsMultiQuery(String key,Object val,final int depth) {
    final HashMap<Object,BitSet> visitedSet=new HashMap<Object,BitSet>();
    // TODO Auto-generated method stub
    System.out.println("In Test Query");
    
    PipeFunction<LoopBundle<Vertex>,Boolean> whileFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
//        System.out.println("While Path:" + bundle.getPath() + "," + bundle.getObject().getId());
//        Object rootVertex=bundle.getPath().get(0);
//        Object currentVertex=bundle.getObject();
//        
//        Boolean flag=true;
//        BitSet rootVertexBitSet= visitedSet.get(rootVertex);
//        int pseudoId=(Integer)((Vertex)currentVertex).getProperty("patid");
//        if(rootVertexBitSet==null){
//          rootVertexBitSet = new BitSet();
//
//          System.out.println("setting ID:" + ((Vertex)currentVertex).getId());
//          rootVertexBitSet.set(pseudoId);
//          visitedSet.put(rootVertex, rootVertexBitSet);
//        }
//        else{
//          
//          boolean bit=rootVertexBitSet.get(pseudoId);
//          if(bit==true){
//            System.out.println("Loop found");
//            return false;
//          }
//          else{
//            System.out.println("setting ID:" + ((Vertex)currentVertex).getId());
//            rootVertexBitSet.set(pseudoId);
//          }
//        }
//         System.out.println("returning while" + (bundle.getLoops()< depth+1)); 
//       return (bundle.getLoops()< depth+1);
        return (bundle.getLoops()< depth && bundle.getObject()!=null);
      }
    };
    PipeFunction<LoopBundle<Vertex>,Boolean> emitFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
//       System.out.println("Emitted Path:" + bundle.getPath() + "," + bundle.getObject().getId());
     Object rootVertex=bundle.getPath().get(0);
     Object currentVertex=bundle.getObject();
     
     Boolean flag=true;
     BitSet rootVertexBitSet= visitedSet.get(rootVertex);
     int pseudoId=(Integer)((Vertex)currentVertex).getProperty("rid");
     if(rootVertexBitSet==null){
       
       rootVertexBitSet = new BitSet();
       //setting root vertex and currentVertex as visited as rootVertexBitSet is null
       rootVertexBitSet.set((Integer)((Vertex)rootVertex).getProperty("rid"));
//       System.out.println("setting ID:" + ((Vertex)currentVertex).getId());
       rootVertexBitSet.set(pseudoId);
       visitedSet.put(rootVertex, rootVertexBitSet);
     }
     else{
       
       boolean bit=rootVertexBitSet.get(pseudoId);
       if(bit==true){
//         System.out.println("Loop found");
         flag =false;
       }
       else{
//         System.out.println("setting ID:" + ((Vertex)currentVertex).getId());
         rootVertexBitSet.set(pseudoId);
       }
     }

       return flag; 
      }
    };
    
//    PipeFunction<Vertex,Boolean> filterFunction = new PipeFunction<Vertex,Boolean>(){
//
//      @Override
//      public Boolean compute(Vertex v) {
//       return ; 
//      }
//    };
    
    
    long t1= System.currentTimeMillis();
    //lazy evaluation
    //GremlinPipeline pipe = new GremlinPipeline(titanGraph).V(key,val).as("x").out().loop("x", whileFunction,emitFunction ).path();
    
  //for non-lazy evaluation
    List bfsMultiPathList = new GremlinPipeline(titanGraph).V(key,val).as("x").out().loop("x", whileFunction,emitFunction ).path().toList();
    
    System.out.println("Time: " + (System.currentTimeMillis()-t1));
    System.out.println("BFS Path Count:" + bfsMultiPathList.size());
    
    for(Object o: bfsMultiPathList){
      System.out.println("Path: " + o.toString());
    }
    System.out.println("Exiting querying");
    
//    BitSet bitSet= visitedSet.get(titanGraph.getVertex(1454592));
//    for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
//      System.out.println("SetBit:" + i);
//  }
//    for(Object o : visitedSet){
//      System.out.println(o.toString());
//    }
  }

  
  
  @SuppressWarnings("unused") 
  public void bfsQuery(String key,Object value,final int depth) {

    final HashSet<Object> visitedSet=new HashSet<>();
    
    PipeFunction<Vertex, Boolean> bfsFilterFunction = new PipeFunction<Vertex, Boolean>() {
      public Boolean compute(Vertex i)
      {
          Object patid = i.getProperty("patid");
          if(visitedSet.contains(i.getProperty("patid"))){
              visitedSet.add(patid);
              return true;
          }
          else
              return false;
      }

  };
    
    
    PipeFunction<LoopBundle<Vertex>,Boolean> whileFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
       return bundle.getLoops()<= depth && bundle.getObject()!= null;
      }

     
      
    };
    

    PipeFunction<LoopBundle<Vertex>,Boolean> emitFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
       return (bundle.getLoops()== depth+1) || (bundle.getLoops()< depth+1 && bundle.getObject().getEdges(Direction.OUT).iterator().hasNext()==false);
      }

     
      
    };
    
    
    GremlinPipeline pipe =new GremlinPipeline(titanGraph).V(key, value).store(visitedSet).as("x").out().filter(bfsFilterFunction).loop("x", whileFunction,emitFunction).path();
    
    System.out.println(pipe.count());

  }
  
  
  public void shortestPath(){
    final Vertex v1 = titanGraph.getVertex(2);
    final Vertex v2 = titanGraph.getVertex(6);
    final Set<Vertex> x = new HashSet<>(Collections.singleton(v1));

    final GremlinPipeline<Object, List> pipe = new GremlinPipeline<>(v1).as("x")
            .both().except(x).store(x).loop("x", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                @Override
                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
                    return !x.contains(v2);
                }
            }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                @Override
                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
                    return bundle.getObject() == v2;
                }
            }).path();
    
    for (final List path : pipe) {
        System.out.println(path);
    }
    
  }
  
  
//  public void bfsQuery(String key, Object val){
//    
//    final Set<Vertex> x = new HashSet<>();
//
//    final GremlinPipeline<Object, List> pipe = new GremlinPipeline<>(titanGraph).V(key, val).as("x")
//            .store(x).out().loop("x", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
//                @Override
//                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
//                    Vertex v =bundle.getObject();
//                    v = Gremlin
//                    
//                }
//            }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
//                @Override
//                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
//                    return bundle.getObject() == v2;
//                }
//            }).path();
//    for (final List path : pipe) {
//        System.out.println(path);
//    }
//    
//    
//  }
   


   
 }



/*AZURE TITAN CONFIGURATION
conf = new BaseConfiguration();
conf.setProperty("storage.backend","cassandra");
conf.setProperty("storage.cassandra.keyspace","recreated");
conf.setProperty("storage.connection-timeout","5000000");
conf.setProperty("storage.setup-wait","2000000");
conf.setProperty("index.search.backend","elasticsearch");
conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
conf.setProperty("cache.db-cache","true");
g = TitanFactory.open(conf);

*/


/*
 * titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open(); 
 */
