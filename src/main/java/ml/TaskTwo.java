package ml;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Created by JIANG on 2017/5/24.
 */
public class TaskTwo {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String measurementsDir = params.getRequired("measurements-dir");
        if(measurementsDir.charAt(measurementsDir.length() - 1) != '/') {
            measurementsDir = measurementsDir + '/';
        }

        // Read in the ratings file.
        // Now this only works for /share/cytometry/small
        // TODO: Read muliple files
        DataSet<String> measurementsRaw = env.readTextFile(measurementsDir + "measurements_arcsin200_p1.csv");

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


    }
}
