package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

public class FlightSolver extends AbstractFlightAnalyser<Path>{

	/**
	 * The representation of the graph
	 */
	private CoordinateMatrix graph;

	public FlightSolver(JavaRDD<Flight> flights)  {
		super(flights);
		this.graph = new CoordinateMatrix(this.createGraph().rdd());
	}

	/**
	 *
	 * @return
	 */
	private JavaPairRDD<Tuple2<Long, Long>, Double> allAirportCombinations() {
		JavaRDD<Long> allAirports = flights.map(f -> f.org_id).union(flights.map(f -> f.dest_id)).distinct();
		JavaPairRDD<Long, Long> pairs = allAirports.cartesian(allAirports);
		return pairs.mapToPair(p -> new Tuple2<>(new Tuple2<>(p._1, p._2), .0));
	}

	/**
	 *
	 * @return
	 */
	public JavaRDD<MatrixEntry> createGraph() {
		// This initializes all the routes
		// with their value at zero
		JavaPairRDD<Tuple2<Long, Long>, Double> allPairsZeros = allAirportCombinations();

		// Calculate the average distances and
		// Aggregate them by their key value
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
		return noDirectPathMaxDist.map(
				t -> new MatrixEntry(t._1._1,t._1._2,t._2)
		);
	}

	/**
	 *
	 * @return
	 */
	@Override
	public Path run() {
		return null;
	}
}
