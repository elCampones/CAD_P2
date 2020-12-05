package cadlabs.rdd;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class AverageFlightDurationTest extends AbstractTest<Map<Tuple2<String, String>, Double>> {
    @Override
    protected Map<Tuple2<String, String>, Double> run(JavaRDD<Flight> flights) {
        return new AverageFlightDuration(flights).run();
    }

    // TODO : Not implemented, don't know the correct result!
    @Override
    protected String expectedResult() {
        return "";
    }
}
