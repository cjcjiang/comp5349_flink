package ml;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.util.Collector;

/**
 * Created by Yuming on 2017/5/26.
 */
public class FlatMapTest extends RichFlatMapFunction<Centroid, Collector<Centroid>> {

    @Override
    public void flatMap(Centroid tuple, Collector<Centroid> out) throws Exception{

    }
}
