import cadlabs.graph.GraphBuilder;
import cadlabs.par.FlightInformer;
import cadlabs.par.ParSSSP;
import cadlabs.rdd.AbstractFlightAnalyser;
import cadlabs.rdd.Flight;
import cadlabs.rdd.Path;
import cadlabs.seq.SSSP;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Collection;
import java.util.List;
import java.util.Scanner;

public class DistanceFinderMain {

    private static final String HELP_MESSAGE = String.format(
            "Commands available:\n" +
                    "\t%s:\tDisplays help menu (this one)\n" +
                    "\t%s:\tReceives the names of two airports in the graph and outputs the ideal route and optimal time between them.\n" +
                    "\t\tThe amount of time required to compute these properties is also presented.\n" +
                    "\t%s: Sets the average flight time between two airports.\n" +
                    "\t\tThe first two parameters are the names of the airports whose distance will be set.\n" +
                    "\t\tThe final parameter is the amount of time a flight between the airports should take.\n" +
                    "\t\tThis final parameter is 4 figure natural number. The first two figures represent the hours and the latter the minutes\n" +
                    "\t%s: Displays the optimal route and time between airports like the info command, but uses a classical Dijkstra shortest path algorithm.\n" +
                    "\t%s: Terminates the program."
            , Command.HELP.commandMatch, Command.COMPUTE_DISTANCE.commandMatch
            , Command.SET_DISTANCE.commandMatch, Command.COMPUTE_SEQ.commandMatch
            , Command.EXIT.commandMatch);

    private enum Command {
        HELP("help"),
        COMPUTE_DISTANCE("info"),
        SET_DISTANCE("time"),
        COMPUTE_SEQ("info_seq"),
        EXIT("exit");

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
        if (args.length < 1)
            System.out.println("Usage: java DistanceFinderMain [FlightsFile]\n "
            		+ "FlightsFile: Pathname of the file containing the flights data\n");
        SparkSession spark = initiateSpark();
        String file = args.length > 3 ? args[2] : "data/flights.csv";
        JavaRDD<Flight> flights = processInputFile(file, spark);
        try(Scanner in = new Scanner(System.in)) {
            new DistanceFinderMain(flights, in);
        }
    }

    private final GraphBuilder graph;

    private final JavaRDD<Flight> flights;

    private final Scanner in;

    public DistanceFinderMain(JavaRDD<Flight> fligths, Scanner in) {
        this.flights = fligths;
        fligths.collect();

//        List<Flight> temp = (List<Flight>)Flight.generateIds(flights.collect());
//        JavaRDD<Flight> temp2 = spark.parallelize(temp);
        FlightInformer.informer.setInformer(fligths);

        graph = new GraphBuilder(fligths);
        this.in = in;
        interpretCommands();
    }

    private static SparkSession initiateSpark() {

        // start Spark session (SparkContext API may also be used)
        // master("local") indicates local execution
        SparkSession spark = SparkSession.builder().
                appName("FlightAnalyser").
                master("local").
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
        SSSP sssp = new SSSP(origin, dest, flights, graph);
        computeDist(sssp);
    }

    private void computeDist(AbstractFlightAnalyser<Path> flightAnalyser) {
        long start = System.currentTimeMillis();
        Path path = flightAnalyser.run();
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

}
