package cadlabs.rdd;

import cadlabs.AbstractBaseTest;
import org.apache.spark.api.java.JavaRDD;


public abstract class AbstractTest<Result> extends AbstractBaseTest<JavaRDD<Flight>, Result> {


    protected JavaRDD<Flight> processInputFile(String file) {
        JavaRDD<String> textFile = spark.read().textFile(file).javaRDD();
        return textFile.map(Flight::parseFlight).cache();
    }


}
