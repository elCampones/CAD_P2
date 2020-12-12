package cadlabs.graph;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import cadlabs.rdd.Flight;
import scala.Tuple2;

public class GraphBuilder {
	
	public static IndexedRowMatrix buildGraph(JavaRDD<Flight> flights) {

        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Double, Integer>> aux1 =
                flights.mapToPair(flight ->
                	new Tuple2<>(new Tuple2<>(flight.origInternalId, flight.destInternalId),
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

        JavaRDD<MatrixEntry> entries =
                flightAverageDuration.map(
                        flight ->new MatrixEntry(flight._1._2, flight._1._1, flight._2));
	
        return new CoordinateMatrix(entries.rdd()).transpose().toIndexedRowMatrix();
	}  
}
