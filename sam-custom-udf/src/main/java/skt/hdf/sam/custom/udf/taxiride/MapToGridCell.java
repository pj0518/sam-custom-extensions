package skt.hdf.sam.custom.udf.taxiride;

import com.hortonworks.streamline.streams.rule.UDF5;

public class MapToGridCell implements UDF5<Integer, Boolean, Double, Double, Double, Double> {

    public static double LON_WEST = -74.05;
    public static double LAT_NORTH = 41.0;

    // delta step to create artificial grid overlay of NYC
    public static double DELTA_LON = 0.0014;
    public static double DELTA_LAT = 0.00125;

    // ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT
    public static int NUMBER_OF_GRID_X = 250;
    // ( LAT_NORTH - LAT_SOUTH ) / DELTA_LON

    @Override
    public Integer evaluate(Boolean isStart, Double startLon, Double startLat, Double endLon, Double endLat) {
        Integer gridId;

        if (isStart) {
            // get grid cell id for start location
            gridId = new Integer(mapToGridCell(startLon, startLat));

        } else {
            // get grid cell id for end location
            gridId = new Integer(mapToGridCell(endLon, endLat));
        }

        return gridId;
    }

    private int mapToGridCell(double lon, double lat) {
        int xIndex = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON);
        int yIndex = (int)Math.floor((LAT_NORTH - lat) / DELTA_LAT);

        return xIndex + (yIndex * NUMBER_OF_GRID_X);
    }
}
