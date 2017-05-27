package ml;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Created by JIANG on 2017/5/25.
 */
public class ClusterPoints extends RichMapFunction<Point, Tuple3<Integer, Point, Double>>{
    private Collection<Centroid> centroids;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("newest_centroids");
    }

    @Override
    public Tuple3<Integer, Point, Double> map(Point p) throws Exception {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        // check all cluster centers
        for (Centroid centroid : centroids) {
            // compute distance
            double distance = p.calculate_euclidean_distance(centroid);

            // update nearest cluster if necessary
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = centroid.id;
            }
        }
        Tuple3<Integer, Point, Double> temp = new Tuple3<>(closestCentroidId, p, minDistance);
        return temp;
    }
}

