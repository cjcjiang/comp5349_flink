package ml;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Created by JIANG on 2017/5/25.
 */
public class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>>{
    private Collection<Centroid> centroids;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Get the broadcast variable: centroids, with the name "newest_centroids"
        this.centroids = getRuntimeContext().getBroadcastVariable("newest_centroids");
    }

    @Override
    public Tuple2<Integer, Point> map(Point p) throws Exception {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        // Check all cluster centers
        for (Centroid centroid : centroids) {
            // Compute distance
            double distance = p.calculate_euclidean_distance(centroid);

            // Update nearest cluster if necessary
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = centroid.id;
            }
        }
        Tuple2<Integer, Point> temp = new Tuple2<>(closestCentroidId, p);
        return temp;
    }
}
