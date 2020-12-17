package cadlabs.rdd;

import cadlabs.sssp.FlightInformer;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that represents a path in the graph
 */
public class Path {

    public final String src;

    public final String dest;

    public final double distance;
    /**
     * The list representation of the path
     */
    private final List<String> path = new ArrayList<>();

    /**
     * The weigth of the path between the two nodes
     */
    private double pathWeight;

    /**
     * Constructor given the source and destination node ids and the list of predecessors
     *
     * @param source
     * @param destination
     * @param predecessor
     */
    public Path(long source, long destination, int[] predecessor, double[] distances) {
        this.src = FlightInformer.informer.mapAirportById.get((int) source);
        this.dest = FlightInformer.informer.mapAirportById.get((int) destination);
        for (int v = (int) destination; v != source; v = predecessor[v])
            this.path.add(0, FlightInformer.informer.mapAirportById.get(v));

        this.path.add(0, FlightInformer.informer.mapAirportById.get((int) source));
        distance = computeDistance(source, destination, distances, predecessor);
    }

    private double computeDistance(long source, long destination, double[] distances, int[] predecessor) {
        int current = (int) destination;
        double accum = 0;
        while (current != source) {
            accum += distances[current];
            current = predecessor[current];
        }

        return accum;
    }

    /**
     * Obtain the path as a list of node names
     *
     * @return The list of node names
     */
    public List<String> getPathAsList() {
        return this.path;
    }


    public double getWeight() {
        return this.pathWeight;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        int last = this.path.size() - 1;

        for (int i = 0; i < last; i++)
            result.append(this.path.get(i)).append(" -> ");

        return result + this.path.get(last);
    }
}
