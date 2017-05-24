package ml;

import java.io.Serializable;

/**
 * Created by JIANG on 2017/5/24.
 */
public class Point implements Serializable {

    public double x, y, z;

    public Point() {}

    public Point(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public double calculate_euclidean_distance(Point another) {
        return Math.sqrt((x-another.x)*(x-another.x) + (y-another.y)*(y-another.y) + (z-another.z)*(z-another.z));
    }

}
