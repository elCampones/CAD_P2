package cadlabs.sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.*;

class Flight {

    private static final Map<Long, Integer> airports = new HashMap<Long, Integer>();
    private static final Map<Integer, String> airportsRev = new HashMap<Integer, String>();
    private static final Map<String, Integer> airportsByName = new HashMap<String, Integer>();
    static StructType schema = DataTypes.createStructType(
            new StructField[]{
                    createStructField("day of Month", StringType, false),
                    createStructField("day of Week", StringType, false),
                    createStructField("carrier", StringType, false),
                    createStructField("tailnum", StringType, false),
                    createStructField("flnum", IntegerType, false),
                    createStructField("org_id", LongType, false),
                    createStructField("origin", StringType, false),
                    createStructField("dest_id", LongType, false),
                    createStructField("dest", StringType, false),
                    createStructField("scheduled dep time", DoubleType, false),
                    createStructField("dep time", DoubleType, false),
                    createStructField("departure delay", DoubleType, false),
                    createStructField("scheduled arr time", DoubleType, false),
                    createStructField("arr time", DoubleType, false),
                    createStructField("arrival delay", DoubleType, false),
                    createStructField("elapsed time", DoubleType, false),
                    createStructField("distance", IntegerType, false),

                    createStructField("origInternalId", LongType, false),
                    createStructField("destInternalId", LongType, false),

            });
    private static int internalIds = 0;

    static Row parseFlight(String line) {
        String[] data = line.split(",");

        long orig_id = Long.parseLong(data[5]);
        String orig = data[6];
        long dest_id = Long.parseLong(data[7]);
        String dest = data[8];

        return RowFactory.create(data[0], data[1], data[2], data[3], Integer.parseInt(data[4]), orig_id,
                orig, dest_id, dest, Double.parseDouble(data[9]), Double.parseDouble(data[10]),
                Double.parseDouble(data[11]), Double.parseDouble(data[12]),
                data[13].equals("") ? 0 : Double.parseDouble(data[13]),
                data[14].equals("") ? 0 : Double.parseDouble(data[14]), Double.parseDouble(data[15]),
                Integer.parseInt(data[16]),
                internalId(orig_id, orig),
                internalId(dest_id, dest));


    }

    static ExpressionEncoder<Row> encoder() {
        return RowEncoder.apply(schema);
    }


    private static long internalId(long airport, String name) {
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