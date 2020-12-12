package cadlabs.par;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;

import cadlabs.graph.GraphBuilder;
import cadlabs.rdd.AbstractFlightAnalyser;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import scala.Tuple2;

public class ParSSSP extends AbstractFlightAnalyser<Path> {
	
	private final String srcName, destName;
	
//	private final IndexedRowMatrix graph;
	private final JavaRDD<IndexedRow> graph;
	
	public ParSSSP(String srcName, String destName, JavaRDD<Flight> flights) {
		super(flights);
		this.srcName = srcName;
        this.destName = destName;
		graph = GraphBuilder.buildGraph(flights).rows().toJavaRDD();
		//for (IndexedRow row : graph.take(2000))
		//	System.out.println(row);
			
	}

	@Override
	public Path run() {
		graph.cache();
		int source = Flight.getAirportIdFromName(srcName);
        int destination = Flight.getAirportIdFromName(destName);
        int nAirports = (int) Flight.getNumberAirports();
        
        double[] shortestPath = new double[nAirports];
        int[] from = new int[nAirports];
        boolean[] confirmed = new boolean[nAirports];
        for (int i = 0; i < shortestPath.length; i++) {
        	shortestPath[i] = Double.MAX_VALUE;
        	from[i] = -1;
        }
        
        shortestPath[source] = 0;
//        from[source] = source;
        
        Queue<Tuple2<Integer, Integer>> toVisit = new PriorityQueue<>(nAirports);
        toVisit.add(new Tuple2<>(0, source));
        do {
        	
        	Integer found = toVisit.peek()._2;
        	if (confirmed[found]) {
        		toVisit.remove();
        		continue;
        	}
        	confirmed[found] = true;
        	
        	Set<Long> roundNodes = toVisit.stream().map(t -> Long.valueOf(t._2)).collect(Collectors.toSet());
        	JavaRDD<IndexedRow> partitionRows =
    				graph
    				.filter(s -> roundNodes.contains(s.index()));
        	
        	JavaPairRDD<Integer/*origin*/, Tuple2<Integer /*dest*/, Integer/*newDistance*/>> modified =
        			partitionRows.flatMapToPair(f -> );
    		
        	
        } while(!confirmed[destination]);
		
		return new Path(source, destination, from);
	}

}
