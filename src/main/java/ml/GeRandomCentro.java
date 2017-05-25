package ml;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Yuming on 2017/5/25.
 */
public class GeRandomCentro extends AbstractRichFunction implements MapPartitionFunction<Tuple6<Double, Double, Double, Double, Double, Double>, Centroid> {
    private IntCounter numLines = new IntCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("num-lines", this.numLines);
        this.numLines.add(1);
    }



    @Override
    public void mapPartition(Iterable<Tuple6<Double, Double, Double, Double, Double, Double>> tuples, Collector<Centroid> out) throws Exception{
        for(Tuple6<Double, Double, Double, Double, Double, Double> tuple : tuples){
            Double x_min = tuple.f0;
            Double x_max = tuple.f1;
            Double y_min = tuple.f2;
            Double y_max = tuple.f3;
            Double z_min = tuple.f4;
            Double z_max = tuple.f5;
            Double x_random = ThreadLocalRandom.current().nextDouble(x_min, x_max);
            Double y_random = ThreadLocalRandom.current().nextDouble(y_min, y_max);
            Double z_random = ThreadLocalRandom.current().nextDouble(z_min, z_max);
            int id = numLines.getLocalValuePrimitive();
            Centroid temp = new Centroid(id, x_random, y_random, z_random);
            out.collect(temp);
        }
    }
}
