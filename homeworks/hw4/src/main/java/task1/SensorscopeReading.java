package task1;

import com.google.common.base.Joiner;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by filipgulan on 03/06/2017.
 */
public class SensorscopeReading {

    private Integer stationID;
    private Double solarCurrent;

    public SensorscopeReading(String[] items) {
        this.stationID = Integer.parseInt(items[0]);
        this.solarCurrent = Double.parseDouble(items[16]);
    }

    public Integer getStationID() {
        return stationID;
    }

    public Double getSolarCurrent() {
        return solarCurrent;
    }

    public static boolean isParsable(String[] items) {
        try {
            Integer.parseInt(items[0]);
            Integer.parseInt(items[1]);
            Integer.parseInt(items[2]);
            Integer.parseInt(items[3]);
            Integer.parseInt(items[4]);
            Integer.parseInt(items[5]);
            Integer.parseInt(items[6]);
            Integer.parseInt(items[7]);
            Float.parseFloat(items[8]);
            Float.parseFloat(items[9]);
            Float.parseFloat(items[10]);
            Float.parseFloat(items[11]);
            Float.parseFloat(items[12]);
            Float.parseFloat(items[13]);
            Float.parseFloat(items[14]);
            Float.parseFloat(items[15]);
            Float.parseFloat(items[16]);
            Float.parseFloat(items[17]);
            Float.parseFloat(items[18]);

            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
