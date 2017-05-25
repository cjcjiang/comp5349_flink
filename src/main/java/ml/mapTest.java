package ml;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Yuming on 2017/5/26.
 */
public class mapTest implements MapFunction<Tuple6<Double, Double, Double, Double, Double, Double>, Tuple12<Integer, Double, Double, Double, Integer, Double, Double, Double, Integer, Double, Double, Double>> {
    @Override
    public Tuple12<Integer, Double, Double, Double, Integer, Double, Double, Double, Integer, Double, Double, Double> map(Tuple6<Double, Double, Double, Double, Double, Double> tuple) throws Exception{
        Double x_min = tuple.f0;
        Double x_max = tuple.f1;
        Double y_min = tuple.f2;
        Double y_max = tuple.f3;
        Double z_min = tuple.f4;
        Double z_max = tuple.f5;
        Double x_random_1 = ThreadLocalRandom.current().nextDouble(x_min, x_max);
        Double x_random_2 = ThreadLocalRandom.current().nextDouble(x_min, x_max);
        Double x_random_3 = ThreadLocalRandom.current().nextDouble(x_min, x_max);
        Double y_random_1 = ThreadLocalRandom.current().nextDouble(y_min, y_max);
        Double y_random_2 = ThreadLocalRandom.current().nextDouble(y_min, y_max);
        Double y_random_3 = ThreadLocalRandom.current().nextDouble(y_min, y_max);
        Double z_random_1 = ThreadLocalRandom.current().nextDouble(z_min, z_max);
        Double z_random_2 = ThreadLocalRandom.current().nextDouble(z_min, z_max);
        Double z_random_3 = ThreadLocalRandom.current().nextDouble(z_min, z_max);
        Tuple12<Integer, Double, Double, Double, Integer, Double, Double, Double, Integer, Double, Double, Double> temp = new Tuple12<>(1, x_random_1, y_random_1, z_random_1, 2, x_random_2, y_random_2, z_random_2, 3, x_random_3, y_random_3, z_random_3);
        return temp;
    }
}
