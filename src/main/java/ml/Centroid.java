package ml;

/**
 * Created by JIANG on 2017/5/24.
 */
public class Centroid extends Point{
    public int id;

    public Centroid() {}

    public Centroid(int id, double x, double y, double z) {
        super(x,y,z);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.x, p.y, p.z);
        this.id = id;
    }
}
