package skt.hdf.sam.custom.udf.taxiride;

import com.hortonworks.streamline.streams.rule.UDF4;


public class IsInNYC implements UDF4<Boolean, Double, Double, Double, Double>  {

    public static double LON_EAST = -73.7;
    public static double LON_WEST = -74.05;
    public static double LAT_NORTH = 41.0;
    public static double LAT_SOUTH = 40.5;

    @Override
    public Boolean evaluate(Double startLon, Double startLat, Double endLon, Double endLat) {
        if((!(startLon > LON_EAST || startLon < LON_WEST) &&
                !(startLat > LAT_NORTH || startLat < LAT_SOUTH))&&
                (!(endLon > LON_EAST || endLon < LON_WEST) &&
                        !(endLat > LAT_NORTH || endLat < LAT_SOUTH))){

            return true;
        }
        return false;
    }
}
