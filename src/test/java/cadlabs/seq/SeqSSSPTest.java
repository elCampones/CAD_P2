package cadlabs.seq;

import cadlabs.rdd.AbstractTest;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;


public class SeqSSSPTest extends AbstractTest<Path> {


    @Override
    protected Path run(JavaRDD<Flight> flights) {

        long time = System.currentTimeMillis();

		/*Random r = new Random();
		for(int i = 0; i < 30; i++) {
			int r1 = r.nextInt(300), r2 = r.nextInt(300);
			Path route1 = new SSSP(r1, r2, flights).run();
			Path route2 = new ParSSSP(r1, r2, flights).run();
			if (!route1.toString().equals(route2.toString())) {
				System.out.println("Error Found");
				System.out.println(route1);
				System.out.println(route2);
			}
		}*/
        //Path route = new ParSSSP("SAN", "PSG", flights).run();
        long elapsed = System.currentTimeMillis() - time;
		/*int TPA = Flight.getAirportIdFromName("TPA");
		int ORD = Flight.getAirportIdFromName("ORD");
		int ATL = Flight.getAirportIdFromName("ATL");
		int GRB = Flight.getAirportIdFromName("GRB");
		System.out.println("Airports: TPA=" + TPA + " ORD=" + ORD + " ATL=" + ATL + " GRB=" + GRB);*/
        //System.out.println("Route " + route + "\n Of length " + route.distance + "\nComputed in " + elapsed + " ms.");
        return null;
    }

    @Override
    protected String expectedResult() {
        return null;
    }
}
