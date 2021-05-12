# CAD_P2 - Flight Distance Computation
## Distributed Single Source Shortest Path

### Objective
Computes the single source shortest path in a graph based on a flight record database.
Contains two implementations:
            - Sequential Dijkstra's algorithm.
            - Parallel Johnson's algorithm programmed with the use of the Apache Spark framework.

The purpose of the project was to compare the speedups gained computing the SSSP with a parallel and distributed implementation.

### Tradeoffs

We concluded that there were several tradeoffs in the solutions (explored more in detail in the report)
Dijkstra's implementation has the least amount of work, altough having the highest span.
Local-Parallel Johnson's deployment has more work than Dijkstra's, but can be paralelized and thus gain good performance.
Distributed Johnson's deployment has even more paralelization capabilities than the local deployment. However the communication costs between each superstep are very expensive.

Overall, the local deployment of the Johnson's graph presented the best results, however, the amount of work done and how much can be parallelized in Johnson's depends on the topology of the graph. For graphs where nodes have low degrees, or where the weights of the edges are not uniform, Dijkstra's may perform better.

It's expected that for a bigger graph, the distributed deployment of Johnson would achieve better results, however we didn't manage to test with a graph that big.

### How to run
The Jar cadlabs-spark.jar can be used to run the program in the cluster.  
            With an IDE, the program can be run from the class src/main/java/DistanceFinderMain
