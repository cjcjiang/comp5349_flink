package ml;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by JIANG on 2017/5/25.
 */
public class TaskThree {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final int default_num_iters = 10;
        final String measurement_header= "CD48,Ly6G,CD117,SCA1,CD11b,CD150,CD11c,B220,Ly6C,CD115,CD135,CD3/CD19/NK11,CD16/CD32,CD45";
        final Integer k_num;
        final String task_two_result_dir = "hdfs:////user/yjia4072/task_two_result";

        int num_iters = 0;
        // Get the number of the iterations
        if(params.has("num_iters")){
            num_iters = Integer.parseInt(params.getRequired("num_iters"));
            System.out.println("The number of iterations is set as: " + num_iters);
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
        }else{
            num_iters = default_num_iters;
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.err.println("No num_iters found, The number of iterations is set as default: " + num_iters);
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
        }

        // Get the default three dimensions
        // SCA1,CD11b,Ly6C -> 00110001000000
        // final -> 11100110001000000
        ArrayList<String> default_dimensions = new ArrayList<>();
        ArrayList<String> dimensions = new ArrayList<>();
        default_dimensions.add("SCA1");
        default_dimensions.add("CD11b");
        default_dimensions.add("Ly6C");
        String[] measurement_header_array = measurement_header.split(",");
        String input_field_string = "";
        String input_field_string_default = "";
        for(String header : measurement_header_array){
            if(default_dimensions.contains(header)){
                input_field_string_default = input_field_string_default + "1";
            }else{
                input_field_string_default = input_field_string_default + "0";
            }
        }

        System.err.println("The 111 plus input_field_string_default is: 111 plus " + input_field_string_default);
        System.out.println("####################################################");
        System.out.println("####################################################");
        System.out.println("####################################################");
        System.out.println("####################################################");
        System.out.println("####################################################");

        // Handle the user define dimension order
        Map<Integer, String> dimension_order_user_map = new HashMap<>();
        Map<String, Integer> dimension_order_actual_map = new HashMap<>();
        Map<Integer, Integer> final_order_map = new HashMap<>();
        ArrayList<String> dimension_actual_order = new ArrayList<>();

        // Get the field name that will be used as the dimension
        if(params.has("dimension_name")){
            String dimension_name = params.getRequired("dimension_name");
            String[] dimension_name_array = dimension_name.split(",");
            int dimension_name_array_leng = dimension_name_array.length;
            // By now, only three dimensions can be handled
            // TODO: Have n dimensions to compute
            if(dimension_name_array_leng == 3){
                String dimension_one = dimension_name_array[0];
                String dimension_two = dimension_name_array[1];
                String dimension_three = dimension_name_array[2];

                dimension_order_user_map.put(1, dimension_one);
                dimension_order_user_map.put(2, dimension_two);
                dimension_order_user_map.put(3, dimension_three);

                dimensions = new ArrayList<>();
                dimensions.add(dimension_one);
                dimensions.add(dimension_two);
                dimensions.add(dimension_three);
                for(String header : measurement_header_array){
                    if(dimensions.contains(header)){
                        input_field_string = input_field_string + "1";
                        dimension_actual_order.add(header);
                    }else{
                        input_field_string = input_field_string + "0";
                    }
                }
                input_field_string = "111" + input_field_string;

                // Have the field number for each header with the actual order
                for(int i=0; i<dimension_actual_order.size();i++){
                    String header = dimension_actual_order.get(i);
                    dimension_order_actual_map.put(header, i+3);
                }

                // Link the actual order to the user define order
                for(int i =1; i<=dimension_order_user_map.size(); i++){
                    String header = dimension_order_user_map.get(i);
                    int field_num = dimension_order_actual_map.get(header);
                    final_order_map.put(i, field_num);
                }

                System.out.println("The input_field_string is: " + input_field_string);
                System.out.println("####################################################");
                System.out.println("####################################################");
                System.out.println("####################################################");
                System.out.println("####################################################");
                System.out.println("####################################################");
            }else{
                input_field_string = "111" + input_field_string_default;

                // Have the default order
                for(int i =1; i<=3; i++){
                    int field_num = i + 2;
                    final_order_map.put(i, field_num);
                }

                System.err.println("User did not define the dimension names, the result will be wrong");
                System.out.println("####################################################");
                System.out.println("####################################################");
                System.out.println("####################################################");
                System.out.println("####################################################");
                System.out.println("####################################################");
            }
        }else{
            input_field_string = "111" + input_field_string_default;

            // Have the default order
            for(int i =1; i<=3; i++){
                int field_num = i + 2;
                final_order_map.put(i, field_num);
            }

            System.err.println("User did not define the dimension names, the result will be wrong");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
            System.out.println("####################################################");
        }

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String measurementsDir = params.getRequired("measurements-dir");
        if(measurementsDir.charAt(measurementsDir.length() - 1) != '/') {
            measurementsDir = measurementsDir + '/';
        }

        // Read in the measurements file.
        // All the files under the folder will be read
        // (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
        DataSet<Tuple6<String,Integer,Integer,Double,Double,Double>> measurementsRaw =
                env.readCsvFile(measurementsDir)
                        // .ignoreFirstLine()
                        .ignoreInvalidLines()
                        .includeFields(input_field_string)
                        .types(String.class, Integer.class,Integer.class, Double.class, Double.class, Double.class);

        // Filter out the correct measurement, output:
        // (sample, FSC-A, SSC-A, SCA1, CD11b, Ly6C)
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
                            int field_num_one = final_order_map.get(1);
                            int field_num_two = final_order_map.get(2);
                            int field_num_three = final_order_map.get(3);
                            measurement = new Point(tuple.getField(field_num_one),tuple.getField(field_num_two),tuple.getField(field_num_three));
                            return measurement;
                        });

        DataSet<Centroid> centroids_task_two = env
                .readTextFile(task_two_result_dir)
                .flatMap((line, out) -> {
                    String[] values = line.split("\t");
                    int id = Integer.parseInt(values[0]);
                    Long num_of_points = Long.parseLong(values[1]);
                    double x = Double.parseDouble(values[2]);
                    double y = Double.parseDouble(values[3]);
                    double z = Double.parseDouble(values[4]);
                    Centroid temp = new Centroid(id, x, y, z, num_of_points);
                    out.collect(temp);
                });

        // Attach the cluster id and distance to each point
        DataSet<Tuple3<Integer, Point, Double>> clusteredPoints = measurementsPoint
                // assign points to task two clusters
                .map(new ClusterPoints())
                .withBroadcastSet(centroids_task_two, "newest_centroids");

        // Divide all points into each clusters


    }
}
