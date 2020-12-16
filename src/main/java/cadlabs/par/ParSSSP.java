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
        //for (IndexedRow row : graph.take(2000))
        //	System.out.println(row);

    }

    @Override
    public Path run() {
        graph.cache();
        int source = Flight.getAirportIdFromName(srcName);
        int destination = Flight.getAirportIdFromName(destName);
        //int source = srcName, destination = destName;
        int nAirports = (int) Flight.getNumberAirports();

        Double[] shortestPath = new Double[nAirports];
        int[] from = new int[nAirports];
        boolean[] confirmed = new boolean[nAirports];
        for (int i = 0; i < shortestPath.length; i++) {
            shortestPath[i] = Double.MAX_VALUE;
            from[i] = -1;
        }

        shortestPath[source] = .0;
//        from[source] = source;

        Queue<Pair> toVisit = new PriorityQueue<>(nAirports);
        toVisit.add(new Pair(0, source));
        int found;
        int iterations = 0;
        while (!toVisit.isEmpty() && (found = toVisit.peek().node) != destination){

            if (confirmed[found]) {
                toVisit.remove();
                continue;
            }

            Set<Long> frontierNodes = toVisit.stream().map(t -> (long) t.node)
                    .filter(n -> !confirmed[Math.toIntExact(n)])                //TODO This might not be necessary. Confirmed nodes won't be reintroduced to the priority queue (I think)
                    .collect(Collectors.toSet());

            toVisit.clear();

            JavaRDD<IndexedRow> partitionRows =
                    graph.filter(s -> frontierNodes.contains(s.index()));
            confirmed[found] = true;

            JavaPairRDD<Integer/*origin*/, Tuple2<Integer/*dest*/, Double/*newDistance*/>> modified =
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
                    ).filter(v -> !confirmed[v._2._1])
                            .filter(v -> (v._2._2 < shortestPath[v._2._1]));

            JavaPairRDD<Integer/*dest*/, Tuple2<Integer/*origin*/, Double/*newDistance*/>> minModifications =
                    modified.mapToPair(m -> new Tuple2<>(m._2._1, new Tuple2<>(m._1, m._2._2)))
                            .reduceByKey((v1, v2) -> (v1._2 < v2._2) ? v1 : v2);

            List<Tuple2<Integer, Tuple2<Integer, Double>>> modifiedManifest = minModifications.collect();

            /*
            * For this reduction locks aren't necessary because the previous lambda expression over the RDDs assures that there is only one modification per destination node
             */
            modifiedManifest.forEach(m -> {
                if (m._2._2 < shortestPath[m._1]) {
                    shortestPath[m._1] = m._2._2;
                    from[m._1] = m._2._1;
                    toVisit.add(new Pair(m._2._2, m._1));
                }
            });
        }

        return new Path(source, destination, from, Arrays.stream(shortestPath).mapToDouble(d -> d).toArray());
    }

}
