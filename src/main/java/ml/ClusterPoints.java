package ml;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Created by JIANG on 2017/5/25.
 */
public class ClusterPoints extends RichMapFunction<Point, Tuple4<Integer, Long, Point, Double>>{
    private Collection<Centroid> centroids;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids_task_two");
    }

    @Override
    public Tuple4<Integer, Long, Point, Double> map(Point p) throws Exception {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        Long num_of_points = 0L;
        // check all cluster centers
        for (Centroid centroid : centroids) {
            // compute distance
            double distance = p.calculate_euclidean_distance(centroid);

            // update nearest cluster if necessary
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = centroid.id;
                num_of_points= centroid.num_of_points;
            }
        }
        Tuple4<Integer, Long, Point, Double> temp = new Tuple4<>(closestCentroidId, num_of_points, p, minDistance);
        return temp;
    }
}

