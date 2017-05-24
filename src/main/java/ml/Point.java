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

    public Point add(Point another) {
        x += another.x;
        y += another.y;
        z += another.z;
        return this;
    }

    public Point div(long val) {
        x /= val;
        y /= val;
        z /= val;
        return this;
    }

    @Override
    public String toString() {
        return x + " " + y + " " + z;
    }

}
