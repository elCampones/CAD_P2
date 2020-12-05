package cadlabs.rdd;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class GraphBuilder extends AbstractFlightAnalyser<List<List<Long>>>{
	
	public GraphBuilder(JavaRDD<Flight> flights)  {
		super(flights);
	}

	private JavaPairRDD<Tuple2<String, String>, Double> allAirportCombinations() {
		JavaRDD<String> allAirports =
				flights.map(f -> f.origin).union(flights.map(f -> f.dest)).distinct();
		JavaPairRDD<String, String> pairs = allAirports.cartesian(allAirports);
		JavaPairRDD<Tuple2<String,String>, Double> allPairsZeros = 
				pairs.mapToPair(p -> new Tuple2<>(new Tuple2<>(p._1, p._2), .0));
		return allPairsZeros;
	}
	
	private JavaPairRDD<Tuple2<String, String>, Double> computeAverageDistances() {
		JavaPairRDD<Tuple2<String, String>, Double> allFlightDistances =
				flights.mapToPair(f -> new Tuple2<>(new Tuple2<>(f.origin, f.dest), f.arrtime - f.deptime));
		JavaPairRDD<Tuple2<String, String>, Double> allFlightsAvg =
				allFlightDistances.aggregateByKey(zeroValue, seqFunc, combFunc);
		JavaPairRDD<Tuple2<String, String>, Double> reverseCombinations =
				allFlightsAvg.mapToPair(f -> new Tuple2<>(new Tuple2<>(f._1._2, f._1._1), f._2));
		return allFlightsAvg.union(reverseCombinations);
	}
	
	@Override
	public List<List<Long>> run() {
		JavaPairRDD<Tuple2<String,String>, Double> allPairsZeros = allAirportCombinations();
		JavaPairRDD<Tuple2<String,String>, Double> averageDistances = computeAverageDistances();		
		JavaPairRDD<Tuple2<String,String>, Double> distanceEveryTwoAirports =
				allPairsZeros.union(averageDistances).reduceByKey((i1, i2) -> i1 + i2);
		JavaPairRDD<Tuple2<String,String>, Double> noDirectPathMaxDist =
				distanceEveryTwoAirports.mapToPair(f -> {
					if (f._2 != 0) return f;
					double dist = f._1._1.equals(f._1._2) ? 0 : Double.MAX_VALUE;
					return new Tuple2<>(f._1, dist);
				});
		
		return null;
	}

}
