package ml;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartitioner implements Partitioner<Integer> {

    @Override
    public int partition(Integer key, int numPartitions) {
        if(key<=1000000){
            return 2;
        }else if((key>1000000) && (key<=1500000)){
            return 1;
        }
        else{
            return 0;
        }
    }
}