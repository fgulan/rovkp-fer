package task2;

/**
 * Created by filipgulan on 04/06/2017.
 */
public class USBabyNameRecord {

    private Integer id;
    private String name;
    private Integer year;
    private String gender;
    private String state;
    private Integer count;
    private boolean female;

    public USBabyNameRecord(String[] items) {
        this.id = Integer.parseInt(items[0]);
        this.name = items[1];
        this.year = Integer.parseInt(items[2]);
        this.gender = items[3];
        this.state = items[4];
        this.count = Integer.parseInt(items[5]);
        this.female = gender.equals("F");
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Integer getYear() {
        return year;
    }

    public String getGender() {
        return gender;
    }

    public Boolean isFemale() {
        return female;
    }

    public String getState() {
        return state;
    }

    public Integer getCount() {
        return count;
    }

    public static boolean isParsable(String[] items) {
        try {
            Integer.parseInt(items[0]);
            Integer.parseInt(items[2]);
            Integer.parseInt(items[5]);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
