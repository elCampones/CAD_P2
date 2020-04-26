package cadlabs.rdd;

import java.util.HashMap;
import java.util.Map;

public class Flight {

	private static final Map<Long, Integer> airports = new HashMap<Long, Integer>();
	private static final Map<Integer, String> airportsRev = new HashMap<Integer, String>();
	private static final Map<String, Integer> airportsByName = new HashMap<String, Integer>();
	private static int internalIds = 0;
	
	final String dofM;
	final String dofW;
	final String carrier;
	final String tailnum;
	final int flnum;
	final long org_id;
	final String origin;
	final long dest_id;
	final String dest;
	final double crsdeptime;
	final double deptime;
	final double depdelaymins;
	final double crsarrtime;
	final double arrtime;
	final double arrdelay;
	final double crselapsedtime;
	final int dist;
	final long origInternalId; 
	final long destInternalId; 

	public Flight(String dofM, String dofW, String carrier, String tailnum, int flnum, long org_id, String origin,
			long dest_id, String dest, double crsdeptime, double deptime, double depdelaymins, double crsarrtime,
			double arrtime, double arrdelay, double crselapsedtime, int dist) {

		this.dofM = dofM;
		this.dofW = dofW;
		this.carrier = carrier;
		this.tailnum = tailnum;
		this.flnum = flnum;
		this.org_id = org_id;
		this.origin = origin;
		this.dest_id = dest_id;
		this.dest = dest;
		this.crsdeptime = crsdeptime;
		this.deptime = deptime;
		this.depdelaymins = depdelaymins;
		this.crsarrtime = crsarrtime;
		this.arrtime = arrtime;
		this.arrdelay = arrdelay;
		this.crselapsedtime = crselapsedtime;
		this.dist = dist;
		
		this.origInternalId = internalId(this.org_id, this.origin);
		this.destInternalId = internalId(this.dest_id, this.dest);

	}

	static Flight parseFlight(String line) {
		String[] data = line.split(",");
		return new Flight(data[0], data[1], data[2], data[3], Integer.parseInt(data[4]), Long.parseLong(data[5]),
				data[6], Long.parseLong(data[7]), data[8], Double.parseDouble(data[9]), Double.parseDouble(data[10]),
				Double.parseDouble(data[11]), Double.parseDouble(data[12]),
				data[13].equals("") ? 0 : Double.parseDouble(data[13]),
				data[14].equals("") ? 0 : Double.parseDouble(data[14]), Double.parseDouble(data[15]),
				Integer.parseInt(data[16]));
	
	}
	
	private static long internalId(long airport, String name)  {
		synchronized (airports) {
			Integer id = airports.get(airport);
			if (id == null) {
				id = internalIds++;
				airports.put(airport, id);
				airportsRev.put(id, name);
				airportsByName.put(name, id);
			}
			return id;
		}
	}
}