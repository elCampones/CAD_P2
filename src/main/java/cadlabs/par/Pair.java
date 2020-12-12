package cadlabs.par;

public class Pair implements Comparable<Pair>{

    public final double dist;
    public final int node;

    public Pair(double dist, int node) {
        this.dist = dist;
        this.node = node;
    }

    @Override
    public int compareTo(Pair o) {
        return (int)(this.dist - o.dist);
    }
}
