package cadlabs.sssp;

import cadlabs.rdd.Flight;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class GraphBuilder {

    private JavaRDD<MatrixEntry> graph;

    private final JavaSparkContext spark;

    public GraphBuilder(JavaRDD<Flight> fligths, JavaSparkContext spark) {
        this.spark = spark;
        this.graph = buildGraph(fligths);
    }

    /**
     * Build the graph using Spark for convenience
     *
     * @return The graph
     */
    private static JavaRDD<MatrixEntry> buildGraph(JavaRDD<Flight> flights) {

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Integer>> aux1 =
                flights.
                        mapToPair(
                                flight ->
                                        new Tuple2<>(
                                                new Tuple2<>(flight.origInternalId, flight.destInternalId),
                                                new Tuple2<>(flight.arrtime - flight.deptime < 0 ?
                                                        flight.arrtime - flight.deptime + 2400 :
                                                        flight.arrtime - flight.deptime, 1)));

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Integer>> aux2 =
                aux1.reduceByKey((duration1, duration2) ->
                        new Tuple2<>(duration1._1 + duration2._1,
                                duration1._2 + duration2._2));

        JavaPairRDD<Tuple2<Long, Long>, Double> flightAverageDuration =
                aux2.mapToPair(flightDuration ->
                        new Tuple2<>(flightDuration._1, flightDuration._2._1 / flightDuration._2._2));

        return flightAverageDuration
                .map(flight -> new MatrixEntry(flight._1._2, flight._1._1, flight._2));
    }

    public List<MatrixEntry> getMaterializedGraph() {
        return graph.collect();
    }

    public JavaRDD<IndexedRow> getSparkGraph() {
        return new CoordinateMatrix(graph.rdd()).toIndexedRowMatrix().rows().toJavaRDD();
    }

    public void updateDistance(String orig, String dest, double dist) {
        long o = Flight.getAirportIdFromName(orig);
        long d = Flight.getAirportIdFromName(dest);
        boolean found = false;
        if (existsEdge(o, d))
            graph = graph.map(e -> {
                if ((e.i() == o && e.j() == d)
                        || (e.i() == d && e.j() == o)) {
                    return new MatrixEntry(e.i(), e.j(), dist);
                }
                return e;
            });
        else {
            List<MatrixEntry> newEdges = new ArrayList<>(2);
            newEdges.add(new MatrixEntry(o, d, dist));
            newEdges.add(new MatrixEntry(d, o, dist));
            graph = graph.union(spark.parallelize(newEdges));
        }
    }

    private boolean existsEdge(long orig, long dest) {
        return !graph
                .filter(m -> (m.i() == orig && m.j() == dest) || (m.i() == dest && m.j() == orig))
                .collect().isEmpty();
    }
}
