package cadlabs.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.random.RandomRDDs;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class MatrixOp {
	
	/**
	 * Generate a random matrix of size numRowsxnumCols partitioned into blocks 
	 * of size blockSizexblockSize
	 * 
	 * @param sparkContext Java Spark context
	 * @param numRows number of rows
	 * @param numCols number of columns
	 * @param blockSize block size
	 * @return The block partitioned matrix
	 */
	static JavaRDD<Tuple2<Tuple2<Integer, Integer>, Matrix>> generateMatrix(
			JavaSparkContext sparkContext, int numRows, int numCols, int blockSize) {
		
		// Generate a random sparse matrix

		// Generate the coordinates where to place values
		JavaRDD<Integer> rowindexes = RandomRDDs.normalJavaRDD(sparkContext, numRows).
				map(d -> Math.abs((int) (d*1000)%numRows));
		JavaRDD<Integer> columnindexes = RandomRDDs.normalJavaRDD(sparkContext, numCols).
				map(d -> Math.abs((int) (d*1000)%numCols));	
		JavaPairRDD<Integer, Integer> entries = rowindexes.zip(columnindexes);
		
		System.out.println("--------- Coordinates:");
		for (Tuple2<Integer, Integer> tuple :  entries.collect()) 
			System.out.println("(" + tuple._1 + ", " + tuple._2 + ")");
		
		
		// Create coordinate matrix (i,j) -> i+j
		CoordinateMatrix coordMat = 
				new CoordinateMatrix(((JavaRDD<MatrixEntry>) entries.
						map(e -> new MatrixEntry(e._1, e._2, e._1+e._2))).rdd());		
		
		System.out.println("--------- Coordinate matrix:");
		for (MatrixEntry tuple :  coordMat.entries().toJavaRDD().collect()) 
			System.out.println("(" + tuple.i() + ", " + tuple.j() + "):  " + tuple.value());
		
		// Create the division into blocks of size blockSizexblockSize
		// The matrix created is a matrix whose indexes are of type scala.Int
		// to make it usable in Java we will have to convert to Integers
		JavaRDD<Tuple2<Tuple2<Object, Object>, Matrix>> scalaMatrix = 
				coordMat.toBlockMatrix(blockSize, blockSize).blocks().toJavaRDD();
		
		return scalaMatrix.map(b -> new Tuple2<Tuple2<Integer, Integer>, Matrix>(
										new Tuple2<Integer, Integer>(
											b._1._1$mcI$sp(), b._1._2$mcI$sp()), 
										b._2));
	}
	

	public static void main(String[] args) {

		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession spark = SparkSession.builder().
				appName("WordCount").
				master("local").
				getOrCreate();
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");

		// Generate a matrix of size 10x10 partitioned into blocks of size 2x2
		JavaRDD<Tuple2<Tuple2<Integer, Integer>, Matrix>> blocks = 
				generateMatrix(new JavaSparkContext(spark.sparkContext()), 10, 10, 2);

		System.out.println("--------- Block matrix:");		
		for (Tuple2<Tuple2<Integer, Integer>, Matrix> tuple : blocks.collect()) 
			System.out.println(tuple._1() + ": " + tuple._2());
		
		// Add one to all elements of the matrix
		blocks = blocks.map(b -> new Tuple2<Tuple2<Integer, Integer>, Matrix>(
				b._1, 
				b._2.map(e -> (Double)(e)+1)));
		
		System.out.println("--------- Result block matrix:");	
		for (Tuple2<Tuple2<Integer, Integer>, Matrix> tuple : blocks.collect()) 
			System.out.println(tuple._1() + ": " + tuple._2());

		spark.stop();
	}
}

