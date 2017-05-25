package ml;

/**
 * Created by JIANG on 2017/5/24.
 */
public class Centroid extends Point{
    public int id;
    public Long num_of_points;

    public Centroid() {}

    public Centroid(int id, double x, double y, double z) {
        super(x,y,z);
        this.id = id;
        num_of_points = 0L;
    }

    public Centroid(int id, double x, double y, double z, Long num_of_points) {
        super(x,y,z);
        this.id = id;
        this.num_of_points = num_of_points;
    }

    public Centroid(int id, Point p) {
        super(p.x, p.y, p.z);
        this.id = id;
    }

    public Centroid(int id, Point p, Long num_of_points) {
        super(p.x, p.y, p.z);
        this.id = id;
        this.num_of_points = num_of_points;
    }

    public Centroid(int id, Centroid centroid_no_id) {
        super(centroid_no_id.x, centroid_no_id.y, centroid_no_id.z);
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString() + " The number of the points in this cluster is: " + num_of_points;
    }
}
