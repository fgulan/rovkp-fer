package util;

public class DEBSRecordParser {

    private static final Double UPPER_LONGITUDE = -73.95;
    private static final Double LOWER_LONGITUDE = -74.0;
    private static final Double UPPER_LATITUDE = 40.8;
    private static final Double LOWER_LATITUDE = 40.75;

    private String medallion;
    private Integer duration;
    private Boolean inCenter;
    private Integer category;

    public void parse(String record) {
        String[] tokens = record.split(",");
        medallion = tokens[0];
        duration = Integer.parseInt(tokens[8]);
        Double pickupLongitude = Double.parseDouble(tokens[10]);
        Double pickupLatitude = Double.parseDouble(tokens[11]);
        Double dropoffLongitude = Double.parseDouble(tokens[12]);
        Double dropoffLatitude = Double.parseDouble(tokens[13]);
        inCenter = inCenter(pickupLongitude, pickupLatitude,
                dropoffLongitude, dropoffLatitude);
        Integer passengerCount = Integer.parseInt(tokens[7]);

        if (passengerCount == 1) {
            category = 1;
        } else if (passengerCount == 2 || passengerCount == 3) {
            category = 2;
        } else if (passengerCount >= 4) {
            category = 3;
        } else {
            category = -1;
        }
    }

    private Boolean inCenter(Double pickupLongitude, Double pickupLatitude,
                             Double dropoffLongitude, Double dropoffLatitude) {
        return inRange(pickupLongitude, LOWER_LONGITUDE, UPPER_LONGITUDE)
                && inRange(pickupLatitude, LOWER_LATITUDE, UPPER_LATITUDE)
                && inRange(dropoffLongitude, LOWER_LONGITUDE, UPPER_LONGITUDE)
                && inRange(dropoffLatitude, LOWER_LATITUDE, UPPER_LATITUDE);
    }

    private boolean inRange (Double value, Double lowerBound, Double upperBound){
        return value >= lowerBound && value <= upperBound;
    }

    public String getMedallion() {
        return medallion;
    }

    public Integer getDuration() {
        return duration;
    }

    public Boolean isInCenter() {
        return inCenter;
    }

    public Integer getCategory() {
        return category;
    }
}
