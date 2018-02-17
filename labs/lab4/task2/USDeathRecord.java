package lab.task2;

public class USDeathRecord {    
            
    private final int monthOfDeath, age, dayOfWeekOfDeath, mannerOfDeath;
    private final String sex, maritalStatus, autopsy;

    public USDeathRecord(String[] items) throws NumberFormatException {
        monthOfDeath = Integer.parseInt(items[5]);
        sex = items[6];
        age = Integer.parseInt(items[8]);
        maritalStatus = items[15];
        dayOfWeekOfDeath = Integer.parseInt(items[16]);
        mannerOfDeath = Integer.parseInt(items[19]);
        autopsy = items[21];
    }

    public int getMonthOfDeath() {
        return monthOfDeath;
    }

    public String getGender() {
        return sex;
    }

    public int getAge() {
        return age;
    }

    public String getMaritalStatus() {
        return maritalStatus;
    }

    public int getDayOfWeekOfDeath() {
        return dayOfWeekOfDeath;
    }

    public int getMannerOfDeath() {
        return mannerOfDeath;
    }

    public String getAutopsy() {
        return autopsy;
    }

    public boolean isMale() {
        return sex.equals("M");
    }

    public boolean isFemale() {
        return sex.equals("F");
    }

    public boolean autopsyDone() {
        return autopsy.equals("Y");
    }

    public boolean isMarried() {
        return maritalStatus.equals("M");
    }

    public boolean isAccident() {
        return mannerOfDeath == 1;
    }

    public static boolean isParsable(String[] items) {
        try {
            Integer.parseInt(items[5]);
            Integer.parseInt(items[8]);
            Integer.parseInt(items[16]);
            Integer.parseInt(items[19]);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}