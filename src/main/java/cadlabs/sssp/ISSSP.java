package cadlabs.sssp;

import cadlabs.rdd.Path;

public interface ISSSP {

    Path run();

    Path run(int source, int destination);
}
