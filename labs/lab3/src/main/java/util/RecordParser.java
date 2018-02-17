package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by filipgulan on 13/04/2017.
 */
public class RecordParser {

    private static final double GRID_LENGTH = 0.008983112;
    private static final double GRID_WIDTH = 0.011972;

    private static final Double UPPER_LONGITUDE = -73.117785;
    private static final Double LOWER_LONGITUDE = -74.913585;
    private static final Double UPPER_LATITUDE = 41.474937;
    private static final Double LOWER_LATITUDE = 40.1274702;

    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Double totalAmount;
    private Boolean inArea;
    private Integer hour;
    private String cellKey;

    public void parse(String record) throws Exception {
        String[] tokens = record.split(",");
        Double pickupLongitude = Double.parseDouble(tokens[6]);
        Double pickupLatitude = Double.parseDouble(tokens[7]);
        Double dropoffLongitude = Double.parseDouble(tokens[8]);
        Double dropoffLatitude = Double.parseDouble(tokens[9]);
        totalAmount = Double.parseDouble(tokens[16]);
        inArea = inArea(pickupLongitude, pickupLatitude, dropoffLongitude, dropoffLatitude);

        String dateString = tokens[2];
        Date date = FORMATTER.parse(dateString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        hour = calendar.get(Calendar.HOUR_OF_DAY);
        cellKey = getCellId(pickupLongitude, pickupLatitude);
    }

    private Boolean inArea(Double pickupLongitude, Double pickupLatitude,
                             Double dropoffLongitude, Double dropoffLatitude) {
        return inRange(pickupLongitude, LOWER_LONGITUDE, UPPER_LONGITUDE)
                && inRange(pickupLatitude, LOWER_LATITUDE, UPPER_LATITUDE)
                && inRange(dropoffLongitude, LOWER_LONGITUDE, UPPER_LONGITUDE)
                && inRange(dropoffLatitude, LOWER_LATITUDE, UPPER_LATITUDE);
    }

    public static String getCellId(Double longitude, Double latitude) {
        int pickUpCellIdX = (int)((longitude - LOWER_LONGITUDE) / GRID_WIDTH) + 1;
        int pickUpCellIdY = (int)((UPPER_LATITUDE - latitude) / GRID_LENGTH) + 1;
        return pickUpCellIdX + "." + pickUpCellIdY;
    }

    private boolean inRange (Double value, Double lowerBound, Double upperBound){
        return value >= lowerBound && value <= upperBound;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public Boolean isInArea() {
        return inArea;
    }

    public Integer getHour() {
        return hour;
    }

    public String getCellKey() {
        return cellKey;
    }
}
