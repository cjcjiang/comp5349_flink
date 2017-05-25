package ml;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Created by JIANG on 2017/5/24.
 */
public class TaskTwo {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // TODO: Make the number of iterations can be changed
        final int default_num_iters = 10;

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String measurementsDir = params.getRequired("measurements-dir");
        if(measurementsDir.charAt(measurementsDir.length() - 1) != '/') {
            measurementsDir = measurementsDir + '/';
        }

        // Read in the ratings file.
        // Now this only works for /share/cytometry/small, output
        // (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
        // TODO: Read muliple files
        DataSet<Tuple6<String,Integer,Integer,Double,Double,Double>> measurementsRaw = env.readCsvFile(measurementsDir + "measurements_arcsin200_p1.csv")
                                                                                            // .ignoreFirstLine()
                                                                                            .ignoreInvalidLines()
                                                                                            .includeFields("11100110001000000")
                                                                                            .types(String.class, Integer.class,Integer.class, Double.class, Double.class, Double.class);

        // Filter out the correct measurement, output:
        // (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
        // TODO: If i need to change "Ly6C, CD11b, and SCA1" to something else
        DataSet<Tuple6<String,Integer,Integer,Double,Double,Double>> measurementsHandled =
                measurementsRaw
                        .filter(tuple -> {
                            if((tuple.f1>=1) && (tuple.f1<=150000) && (tuple.f2>=1) && (tuple.f2<=150000)){
                                return true;
                            }
                            else{
                                return false;
                            }});

        // Pick up the useful information out of the measurements
        // (SCA1, CD11b, Ly6C) as (x,y,z)
        DataSet<Point> measurementsPoint =
                measurementsHandled
                        .map(tuple -> {
                            Point measurement;
                            measurement = new Point(tuple.f3,tuple.f4,tuple.f5);
                            return measurement;
                        });

        // TODO: Find three random centroids and broadcast
        Centroid centroid_a = new Centroid(1, 0.5, 0.5, 0.5);
        Centroid centroid_b = new Centroid(2, 1.5, 1.5, 1.5);
        Centroid centroid_c = new Centroid(3, 2.5, 2.5, 2.5);
        DataSet<Centroid> centroids_default = env.fromElements(centroid_a, centroid_b, centroid_c);

        IterativeDataSet<Centroid> loop = centroids_default.iterate(default_num_iters);

        DataSet<Centroid> intermediate_centroids = measurementsPoint
                // compute the closest centroid for each point
                .map(new SelectNearestCenter()
//                        new RichMapFunction<Point, Tuple2<Integer, Point>>(){
//                    private Collection<Centroid> centroids;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        this.centroids = getRuntimeContext().getBroadcastVariable("newest_centroids");
//                    }
//
//                    @Override
//                    public Tuple2<Integer, Point> map(Point p) throws Exception{
//                        double minDistance = Double.MAX_VALUE;
//                        int closestCentroidId = -1;
//                        // check all cluster centers
//                        for (Centroid centroid : centroids) {
//                            // compute distance
//                            double distance = p.calculate_euclidean_distance(centroid);
//
//                            // update nearest cluster if necessary
//                            if (distance < minDistance) {
//                                minDistance = distance;
//                                closestCentroidId = centroid.id;
//                            }
//                        }
//                        Tuple2<Integer, Point> temp =new Tuple2<>(closestCentroidId, p);
//                        return temp;
//                    }
//                }
                )
                .withBroadcastSet(loop, "newest_centroids")
                // count and sum point coordinates for each centroid
                .map(tuple -> new Tuple3<Integer, Point, Long>(tuple.f0, tuple.f1, 1L))
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple3<Integer, Point, Long>>(){
                    @Override
                    public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
                        return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
                    }
                })
                // compute new centroids from point counts and coordinate sums
                .map(tuple -> {
                    Centroid centroid_temp = new Centroid(tuple.f0, tuple.f1.div(tuple.f2));
                    return centroid_temp;
                });

        // feed new centroids back into next iteration
        DataSet<Centroid> final_centroids = loop.closeWith(intermediate_centroids);

        // End the program by writing the output!
        if(params.has("output")) {
            final_centroids.writeAsCsv(params.get("output"));

            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            final_centroids.print();
        }
    }
}

        /*
        * readTextFile version
        * DataSet<String> measurementsRaw = env.readTextFile(measurementsDir + "measurements_arcsin200_p1.csv");

        // Filter out the correct measurement, output:
        // (sample, FSC-A, SSC-A, Ly6C, CD11b, and SCA1)
        // TODO: If i need to change "Ly6C, CD11b, and SCA1" to something else
        DataSet<Tuple6<String,String,String,String,String,String>> measurementsHandled =
                measurementsRaw
                        .map(line -> {
                            Tuple6<String,String,String,String,String,String> temp_tuple;
                            String[] temp_string_array = line.split(",");
                            temp_tuple = new Tuple6<>(temp_string_array[0],temp_string_array[1],temp_string_array[2],temp_string_array[11],temp_string_array[7],temp_string_array[6]);
                            return temp_tuple;
                        })
                        .filter(tuple -> {
                            if((Integer.parseInt(tuple.f1)>=1) && (Integer.parseInt(tuple.f1)<=150000) && (Integer.parseInt(tuple.f2)>=1) && (Integer.parseInt(tuple.f2)<=150000)){
                                return true;
                            }
                            else{
                                return false;
                            }});

        // Pick up the useful information out of the measurements
        // (Ly6C, CD11b, and SCA1) as (x,y,z)
        DataSet<Point> measurementsPoint =
                measurementsHandled
                        .map(tuple -> {
                            Point measurement;
                            measurement = new Point(Double.parseDouble(tuple.f3),Double.parseDouble(tuple.f4),Double.parseDouble(tuple.f5));
                            return measurement;
                        });


    }*/
