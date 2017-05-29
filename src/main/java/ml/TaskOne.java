package ml;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
public class TaskOne {
    public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	final String experiments_dir = "hdfs:////share/cytometry/experiments.csv";
	Configuration parameters = new Configuration();
    //parameters.setBoolean("recursive.file.enumeration", true);

		// Which directory are we receiving input from?
		// This can be local or on HDFS; just format the path correctly for your OS.
		String measurementsDir = params.getRequired("measurements-dir");
		if(measurementsDir.charAt(measurementsDir.length() - 1) != '/') {
			measurementsDir = measurementsDir + '/';
		}
    // format <sample, researcher>
    DataSet<Tuple2<String, String>> experiments = 
		env.readCsvFile(experiments_dir)
			.includeFields("10000001")
			.types(String.class, String.class);
	// format <sample, FSC-A, SSC-A>
	DataSet<Tuple3<String, Integer, Integer>> data = 
		env.readCsvFile(measurementsDir)
		    .ignoreFirstLine()
			.includeFields("11100000000000000")
			.types(String.class, Integer.class, Integer.class)
			.filter(tuple->{
			if (tuple.f1>=1 && tuple.f1<=150000 && tuple.f2>=1 && tuple.f2<=150000){
				return true;
			}
			else {
				return false;
			}
		});//.withParameters(parameters);
		
	// The format of the DataSet joinResults is <sample, researchers, FSC-A, SSC-A>
	DataSet<Tuple4<String, String, Integer, Integer>> joinResults =
    	experiments.join(data)
			.where(0)
			.equalTo(0)
			.projectFirst(0,1)
			.projectSecond(1,2);
	
	// format <"name 1; name 2">
	DataSet<String> researcher = 
		joinResults
			.map(tuple -> tuple.f1);
			
	// format <Researcher, 1>		
	DataSet<Tuple2<String, Integer>> countResearcher= 
		researcher.flatMap((line,out)->{
			String names[]= line.split("; ");
				if(names.length<2){
					out.collect(new Tuple2<String, Integer>(names[0], 1));
				}
				else{
					for(int i=0; i<names.length; i++){
						out.collect(new Tuple2<String, Integer>(names[i], 1));
				}
			}			
		});
		
	// format <Researcher, numOfMeasurements>
	DataSet<Tuple2<String, Integer>> finalResult =
		countResearcher
			.groupBy(0)
			.reduceGroup((tuples, out) -> {
				String name = "";
				int count = 0;
				// do the count
				for(Tuple2<String, Integer> tuple : tuples){
					name = tuple.f0;
					count+=1;
				}
		      out.collect(new Tuple2<String, Integer>(name, count));
			});

		DataSet<Tuple2<String, Integer>> partitionedData = finalResult.partitionCustom(new CustomPartitioner(), 1);
		DataSet<Tuple2<String, Integer>> finalResult_re = partitionedData
			.sortPartition(1, Order.DESCENDING);

			// End the program by writing the output!
			if(params.has("output")) {
			    finalResult_re.writeAsCsv(params.get("output"),"\n","\t");
					env.execute();
			} 
			else {
				System.err.println("No output location specified; printing first 100.");
				// limit direct printing
				finalResult_re.first(100).print();
				finalResult_re.print();
			}
	}	

}

