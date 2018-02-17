package lab.task1;

/**
 * Created by filipgulan on 10/06/2017.
 */
public class PollutionReading {

    private String[] items;
    private Integer ozone;

    public PollutionReading(String[] items) {
        this.items = items;
        this.ozone = Integer.parseInt(items[0]);
    }

    public String getTimestamp() {
        return items[7];
    }

    public String getStationID() {
        return items[5] + "," + items[6];
    }

    public Integer getOzone() {
        return ozone;
    }

    @Override
    public String toString() {
        return String.join(",", items);
    }

    public static boolean isParsable(String[] items) {
        try {
            Integer.parseInt(items[0]);
            Integer.parseInt(items[1]);
            Integer.parseInt(items[2]);
            Integer.parseInt(items[3]);
            Integer.parseInt(items[4]);
            Double.parseDouble(items[5]);
            Double.parseDouble(items[6]);
            return true;
        } catch (NumberFormatException exception) {
            return false;
        }
    }
}
