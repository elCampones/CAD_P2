package cadlabs.par;

import cadlabs.graph.GraphBuilder;
import cadlabs.rdd.AbstractFlightAnalyser;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class ParSSSP extends AbstractFlightAnalyser<Path> {

    private final String srcName, destName;

    //	private final IndexedRowMatrix graph;
    private final JavaRDD<IndexedRow> graph;

    public ParSSSP(String srcName, String destName, JavaRDD<Flight> flights) {
        super(flights);
        this.srcName = srcName;
        this.destName = destName;
        graph = GraphBuilder.buildGraph(flights).rows().toJavaRDD();
    }

    @Override
    public Path run() {
        graph.cache();
        System.out.println(srcName);
        int source = FlightInformer.informer.mapIdByAirport.get(srcName); //1; // Flight.getAirportIdFromName(srcName);
        int destination = FlightInformer.informer.mapIdByAirport.get(destName); //45; // Flight.getAirportIdFromName(destName);
        //int source = srcName, destination = destName;
        int nAirports = (int) FlightInformer.informer.numberOfAirports;//Flight.getNumberAirports();
        System.out.println(nAirports);

        double[] shortestPath = new double[nAirports];
        int[] from = new int[nAirports];
        boolean[] confirmed = new boolean[nAirports];
        for (int i = 0; i < shortestPath.length; i++) {
            shortestPath[i] = Double.MAX_VALUE;
            from[i] = -1;
        }

        shortestPath[source] = 0;
//        from[source] = source;

        Queue<Pair> toVisit = new PriorityQueue<>(nAirports);
        toVisit.add(new Pair(0, source));
        do {
            int found = toVisit.peek().node;
            if (confirmed[found]) {
                toVisit.remove();
                continue;
            }

            confirmed[found] = true;
            if (found == destination) break;

            Set<Long> roundNodes = toVisit.stream().map(t -> (long) t.node).collect(Collectors.toSet());
            JavaRDD<IndexedRow> partitionRows =
                    graph.filter(s -> roundNodes.contains(s.index()));

            List<IndexedRow> list1 = partitionRows.collect();

            JavaPairRDD<Integer/*origin*/, Tuple2<Integer /*dest*/, Double/*newDistance*/>> modified =
                    partitionRows.flatMapToPair(
                            indexedRow -> {
                                SparseVector v = (SparseVector) indexedRow.vector();
                                int[] indices = v.indices();
                                double[] values = v.values();
                                List<Tuple2<Integer, Tuple2<Integer, Double>>> l = new ArrayList<>(indices.length);
                                for (int i = 0; i < indices.length; i++) {
                                    l.add(new Tuple2<>((int) indexedRow.index(), new Tuple2<>(indices[i], values[i] + shortestPath[(int) indexedRow.index()])));
                                }
                                return l.iterator();
                            }
                    ).filter(v -> (v._2._2 < shortestPath[v._2._1]))
                            .filter(v -> !confirmed[v._2._1]);

            List<Tuple2<Integer, Tuple2<Integer, Double>>> modifiedManifest = modified.collect();
            modifiedManifest.forEach(m -> {
                if (m._2._2 < shortestPath[m._2._1]) {
                    shortestPath[m._2._1] = m._2._2;
                    from[m._2._1] = m._1;
                    toVisit.add(new Pair(m._2._2, m._2._1));
                }
            });

        } while (!confirmed[destination]);

        return new Path(source, destination, from, shortestPath);
    }

}
