package cadlabs.rdd;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

public class GraphBuilder extends AbstractFlightAnalyser<List<List<Long>>>{
	
	public GraphBuilder(JavaRDD<Flight> flights)  {
		super(flights);
	}

	private CoordinateMatrix adjacencyMatrix;

	private JavaPairRDD<Tuple2<Long, Long>, Double> allAirportCombinations() {
		JavaRDD<Long> allAirports = flights.map(f -> f.org_id).union(flights.map(f -> f.dest_id)).distinct();
		JavaPairRDD<Long, Long> pairs = allAirports.cartesian(allAirports);
		return pairs.mapToPair(p -> new Tuple2<>(new Tuple2<>(p._1, p._2), .0));
	}
	
	private JavaPairRDD<Tuple2<Long, Long>, Double> computeAverageDistances() {
		// TODO: See this method later,
		//  don't know how to use aggregate by key
		//  method ...

		// This method starts of by calculating
		// all the distances between every airport
		return null;
//		JavaPairRDD<Tuple2<String, String>, Double> allFlightDistances =
//				flights.mapToPair(f -> new Tuple2<>(new Tuple2<>(f.origin, f.dest), f.arrtime - f.deptime));

//		// Then calculates the average of every key
//		// using the aggregateByKey method
//		JavaPairRDD<Tuple2<String, String>, Double> allFlightsAvg =
//				allFlightDistances.aggregateByKey(0,
//						(v1, v2) -> 0.0, (v1, v2) -> 0d);

//		// then swaps the values and puts them in the resulting "array"
//		JavaPairRDD<Tuple2<String, String>, Double> reverseCombinations =
//				allFlightsAvg.mapToPair(f -> new Tuple2<>(new Tuple2<>(f._1._2, f._1._1), f._2));
//		return allFlightsAvg.union(reverseCombinations);
	}
	
	@Override
	public List<List<Long>> run() {
		// This initializes all the routes
		// with their value at zero
		JavaPairRDD<Tuple2<Long, Long>, Double> allPairsZeros = allAirportCombinations();

		// Calculate the average distances and
		// Aggregate them by their key value
		// TODO: This method might be slower
		//  Then what we would expect
		JavaPairRDD<Tuple2<Long, Long>, Double> averageDistances =
				AverageFlightDuration.getAverageDistancesById(flights);

		// Aggregate all distances
		JavaPairRDD<Tuple2<Long,Long>, Double> distanceEveryTwoAirports =
				allPairsZeros.union(averageDistances).reduceByKey((i1, i2) -> i1 + i2);

		// Attributes the max distance to all the values
		JavaPairRDD<Tuple2<Long,Long>, Double> noDirectPathMaxDist =
				distanceEveryTwoAirports.mapToPair(f -> {
					if (f._2 != 0) return f;
					double dist = f._1._1.equals(f._1._2) ? 0 : Double.MAX_VALUE;
					return new Tuple2<>(f._1, dist);
				});

		// Transform the result distances
		// into a distributed matrix
		// TODO: Precisamos de fazer um
		//  mapeamento correto entre matrizes
		//  as coordenadas da matriz e as routes ...
		JavaRDD<MatrixEntry> entries = noDirectPathMaxDist.map(
				t -> new MatrixEntry(t._1._1,t._1._2,t._2)
		);
		adjacencyMatrix = new CoordinateMatrix(entries.rdd());

		return null;
	}

}
