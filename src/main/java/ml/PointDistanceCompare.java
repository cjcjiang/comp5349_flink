package ml;

/**
 * Created by JIANG on 2017/5/27.
 */
public class PointDistanceCompare extends Point implements Comparable<PointDistanceCompare> {
    public Integer id;
    public Point point;
    public Double distance;

    public PointDistanceCompare(Integer id, Point point, Double distance){
        this.id = id;
        this.point = point;
        this.distance = distance;
    }

    @Override
    public int compareTo(PointDistanceCompare point_distance_compare) {
        //ascending order
        if(point_distance_compare.distance > this.distance){
            return -1;
        }else if(point_distance_compare.distance == this.distance){
            return 0;
        }else{
            return 1;
        }
    }
}
