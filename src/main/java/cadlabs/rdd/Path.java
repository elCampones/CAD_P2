package cadlabs.rdd;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that represents a path in the graph
 */
public class Path {

    /**
     * The list representation of the path
     */
    private final List<String> path = new ArrayList<>();

    public final double distance;

    /**
     * Constructor given the source and destination node ids and the list of predecessors
     * @param source
     * @param destination
     * @param predecessor
     */
    public Path(long source, long destination, int[] predecessor, double[] distances) {
        for (int v =  (int) destination; v != source; v = predecessor[v])
            this.path.add(0, Flight.getAirportNameFromId(v));

        this.path.add(0, Flight.getAirportNameFromId((int) source));
        distance = computeDistance(source, destination, distances, predecessor);
    }

    private double computeDistance(long source, long destination, double[] distances, int[] predecessor) {
        int current = (int)destination;
        double accum = 0;
        while (current != source) {
            accum += distances[current];
            current = predecessor[current];
        }

        return accum;
    }

    /**
     * Obtain the path as a list of node names
     * @return The list of node names
     */
    public List<String> getPathAsList() {
        return this.path;
    }

    @Override
    public String toString() {
        String result = "";
        int last = this.path.size()-1;

        for (int i = 0; i < last; i++)
            result += this.path.get(i) + " -> ";

        return result + this.path.get(last);
    }
}
