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
	Configuration parameters = new Configuration();
    //parameters.setBoolean("recursive.file.enumeration", true);
	
	String dataDir = params.getRequired("dataDir");
	        if(dataDir.charAt(dataDir.length() - 1) != '/') {
            dataDir = dataDir + '/';
        }
		
    DataSet<Tuple2<String, String>> experiments = 
		env.readCsvFile(dataDir + "experiments.csv")
			.includeFields("10000001")
			.types(String.class, String.class);
	DataSet<Tuple3<String, Integer, Integer>> data = 
		env.readCsvFile(dataDir + "large")
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
	
	DataSet<String> researcher = 
		joinResults
			.map(tuple -> tuple.f1);
			
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
	
	DataSet<Tuple2<String, Integer>> finalResult =
		countResearcher
			.groupBy(0)
			.reduceGroup((tuples, out) -> {
				String name = "";
				int count = 0;
				
				for(Tuple2<String, Integer> tuple : tuples){
					name = tuple.f0;
					count+=1;
				}
		      out.collect(new Tuple2<String, Integer>(name, count));
			});

		DataSet<Tuple2<String, Integer>> partitionedData = finalResult.partitionCustom(new CustomPartitioner(), 1);
		DataSet<Tuple2<String, Integer>> finalResult_re = partitionedData
		    //.partitionByRange(1)
			.sortPartition(1, Order.DESCENDING);

/**	DataSet<Tuple2<String, Integer>> sortedFinalResult = 
	    finalResult 
		    .groupBy(0)
			.sortGroup(1, Order.DESCENDING)
			.reduceGroup((tuples, out) -> {
			out.collect(new Tuple2<String, Integer>(tuple.f0, tuple.f1));
			});
**/	
			// End the program by writing the output!
			if(params.has("output")) {
			    finalResult_re.writeAsCsv(params.get("output"),"\n","\t");
					env.execute();
			} 
			else {
				// Always limit direct printing
				// as it requires pooling all resources into the driver.
				System.err.println("No output location specified; printing first 100.");
				finalResult_re.first(100).print();
				finalResult_re.print();
			}
	}	

}

