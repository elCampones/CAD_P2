import cadlabs.rdd.DatasetGenerator;
import cadlabs.sssp.*;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Random;
import java.util.List;
import java.util.Scanner;

public class DistanceFinderMain {

    private static final String HELP_MESSAGE = String.format(
            "Commands available:\n" +
                    "\t%s:\tDisplays help menu (this one)\n" +
                    "\t%s:\tReceives the names of two airports in the graph and outputs the ideal route and optimal time between them.\n" +
                    "\t\t\tThe amount of time required to compute these properties is also presented.\n" +
                    "\t%s: Sets the average flight time between two airports.\n" +
                    "\t\t\tThe first two parameters are the names of the airports whose distance will be set.\n" +
                    "\t\t\tThe final parameter is the amount of time a flight between the airports should take.\n" +
                    "\t\t\tThis final parameter is 4 figure natural number. The first two figures represent the hours and the latter the minutes\n" +
                    "\t%s:\tDisplays the optimal route and time between airports like the info command, but uses a classical Dijkstra shortest path algorithm.\n" +
                    "\t%s:\tDetermines the speedup of the Johnson's algorithm when compared with the sequential Dijkstra's algorithm.\n" +
                    "\t\t\tReceives as parameter the number of measurements to be performed.\n" +
                    "\t%s:\tCompares the routes computed between the two algorithms in order to identify errors in a distributed setting where unit tests are unavailable.\n" +
                    "\t\t\tReceives as parameter the number of computations to be performed.\n" +
                    "\t%s:\tTerminates the program.\n"
            , Command.HELP.commandMatch, Command.COMPUTE_DISTANCE.commandMatch
            , Command.SET_DISTANCE.commandMatch, Command.COMPUTE_SEQ.commandMatch
            , Command.COMPUTE_SPEEDUPS.commandMatch, Command.DEBUG.commandMatch
            , Command.EXIT.commandMatch);

    private enum Command {
        HELP("help"),
        COMPUTE_DISTANCE("info"),
        SET_DISTANCE("time"),
        COMPUTE_SEQ("info_seq"),
        EXIT("exit"),
        COMPUTE_SPEEDUPS("speedup"),
        DEBUG("debug");

        private final String commandMatch;

        Command(String commandMatch) {
            this.commandMatch = commandMatch;
        }

        private static Command getCommand(String userInput) throws Exception {
            for (Command command : Command.values())
                if (command.commandMatch.equals(userInput))
                    return command;
            throw new Exception();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2)
            System.out.println("Usage: java DistanceFinderMain [Master] [FlightsFile]\n "
                    + "Master: the master URI fore creating the SparkSession\n"
                    + "FlightsFile: Pathname of the file containing the flights data\n");

        // Set up the current Spark Session
        String master = args.length < 1 ? "local" : args[0];
        SparkSession spark = initiateSpark(master);

        // Load the flights file
        String file = args.length < 2 ? "data/flights.csv" : args[1];
        JavaRDD<Flight> flights = processInputFile(file, spark);
        //JavaRDD<Flight> flights = new DatasetGenerator(500, 20, 0).build(spark.sparkContext());


        try(Scanner in = new Scanner(System.in)) {
            new DistanceFinderMain(flights, in, spark);
        }
    }

    private final GraphBuilder graph;

    private final JavaSparkContext sparkContext;

    private final JavaRDD<Flight> flights;

    private final Scanner in;

    public DistanceFinderMain(JavaRDD<Flight> fligths, Scanner in, SparkSession sparkSession) {

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Flight> l = (List<Flight>)Flight.generateIds(fligths.collect());

        this.flights = sparkContext.parallelize(l);

        FlightInformer.informer.setInformer(flights);
        graph = new GraphBuilder(flights, sparkContext);

        this.in = in;
        interpretCommands();
    }

    private static SparkSession initiateSpark(String value) {

        // start Spark session (SparkContext API maye also be used)
        // master("local") indicates local execution
        SparkSession spark = SparkSession.builder().
                appName("FlightAnalyser").
                master(value).
//                master("spark://172.30.10.116:7077").
                getOrCreate();

        // only error messages are logged from this point onward
        // comment (or change configuration) if you want the entire log
        spark.sparkContext().setLogLevel("ERROR");

        return spark;
    }

    private static JavaRDD<Flight> processInputFile(String file, SparkSession spark) {
        JavaRDD<String> textFile = spark.read().textFile(file).javaRDD();
        return textFile.map(Flight::parseFlight).cache();
    }

    private void interpretCommands() {
        System.out.println(HELP_MESSAGE);
        Command command;
        do {
            System.out.print("> ");
            try {
                command = Command.getCommand(in.next().toLowerCase().trim());
            } catch (Exception badName) {
                command = Command.HELP;
            }
            processCommand(command);
        } while (command != Command.EXIT);

    }

    private void processCommand(Command command) {
        switch(command) {
            case HELP:
                System.out.println(HELP_MESSAGE);
                in.nextLine();
                break;
            case COMPUTE_DISTANCE:
                computePar();
                break;
            case SET_DISTANCE:
                setDist();
                break;
            case COMPUTE_SEQ:
                computeSeq();
                break;
            case COMPUTE_SPEEDUPS:
                computeSpeedups();
                break;
            case DEBUG:
                computeDebug();
                break;
            case EXIT:
                System.out.println("Program finishing");
        }
    }

    private void computePar() {
        String origin = in.next().toUpperCase().trim();
        String dest = in.nextLine().toUpperCase().trim();
        ParSSSP parSSSP = new ParSSSP(origin, dest, flights, graph);
        computeDist(parSSSP);
    }

    private void computeSeq() {
        String origin = in.next().toUpperCase().trim();
        String dest = in.nextLine().toUpperCase().trim();
        SeqSSSP seqSssp = new SeqSSSP(origin, dest, flights, graph);
        computeDist(seqSssp);
    }

    private void computeDist(ISSSP sssp) {
        long start = System.currentTimeMillis();
        Path path = sssp.run();
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Path from %s to %s: %s\n"
                        + "Computed in %d milliseconds\n",
                path.src, path.dest, path, elapsed);
    }

    private void setDist() {
        String origin = in.next().toUpperCase().trim();
        String dest = in.next().toUpperCase().trim();
        double dist = in.nextDouble();
        in.nextLine();
        graph.updateDistance(origin, dest, dist);
    }

    private void computeSpeedups() {
        int iterations = in.nextInt();
        int numAirports = in.nextInt();
        int percentageConnection = in.nextInt();
        in.nextLine();

        List<Flight> l = new DatasetGenerator(numAirports,
                percentageConnection, new Random().nextInt()).
                build(sparkContext.sc()).collect();
        Flight.generateRandomGraphIds(l);

        JavaRDD<Flight> tempFlights = sparkContext.parallelize(l);
        FlightInformer.informer.setInformer(tempFlights);
        GraphBuilder gb = new GraphBuilder(tempFlights, sparkContext);

        System.out.println("Time measurements for the sequential algorithm:");
        float seqTime = computeAlgorithmTime(new SeqSSSP(tempFlights, gb), iterations, numAirports);

        System.out.println("Time measurements for the parallel algorithm:");
        float parTime = computeAlgorithmTime(new ParSSSP(tempFlights, gb), iterations, numAirports);
        System.out.printf("Achieved speedups: %f.2\n", seqTime / parTime);

        FlightInformer.informer.setInformer(this.flights);
    }

    private float computeAlgorithmTime(ISSSP sssp, int iterations, int airports) {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        float average = 0;
        Random r = new Random();
        for (int i = 0; i < iterations; i++) {
            long start = System.currentTimeMillis();
            sssp.run(r.nextInt(airports), r.nextInt(airports), airports);
            long elapsed = System.currentTimeMillis() - start;
            min = Math.min(min, elapsed); max = Math.max(max, elapsed);
            average = average * (i / (float)(i + 1)) + elapsed * (1 / (float) (i + 1));
        }
        System.out.printf(
                "Min measurement:\t%d\n" +
                "Max measurement:\t%d\n" +
                "Average time:\t%f.2\n\n",
                min, max, average);

        return average;
    }

    private void computeDebug() {
        int measurements = in.nextInt();
        in.nextLine();
        Random r = new Random();
        int airports = (int) FlightInformer.informer.numberOfAirports;
        SeqSSSP correct = new SeqSSSP(flights, graph);
        ParSSSP testing = new ParSSSP(flights, graph);
        boolean found = false;
        for (int i = 0; i < measurements; i++) {
            int orig = r.nextInt(airports), dest = r.nextInt(airports);
            Path seqP = correct.run(orig, dest, airports);
            Path parP = testing.run(orig, dest, airports);
            if (!seqP.toString().equals(parP.toString())) {
                System.out.printf("Error found:\n\tCorrect Path: %s\n\tFound Path: %s\n\n"
                        , seqP, parP);
                found = true;
            }
        }
        if (!found)
            System.out.println("No errors found.");
    }
}
