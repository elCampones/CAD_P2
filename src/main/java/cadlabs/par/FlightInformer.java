package cadlabs.par;

import cadlabs.rdd.Flight;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class FlightInformer {
    private static final FlightInformer _informer = new FlightInformer();
    public static final FlightInformer informer = _informer;

    public Map<String, Integer> mapIdByAirport;
    public Map<Integer, String> mapAirportById;
    public long numberOfAirports;


    private FlightInformer() {
        mapIdByAirport = new HashMap<>();
        mapAirportById = new HashMap<>();
        numberOfAirports = 0;
    }

    public void setInformer(JavaRDD<Flight> flights) {
        mapAirportById = flights.mapToPair(flight -> new Tuple2<>((int)flight.origInternalId, flight.origin))
                .reduceByKey((v1, v2) -> v1).collectAsMap();
        mapIdByAirport = flights.mapToPair(flight -> new Tuple2<>(flight.origin, (int)flight.origInternalId))
                .reduceByKey((v1, v2) -> v1).collectAsMap();
        numberOfAirports = mapIdByAirport.values().size();

//        System.out.println(numberOfAirports);
//        System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH");
    }

}
