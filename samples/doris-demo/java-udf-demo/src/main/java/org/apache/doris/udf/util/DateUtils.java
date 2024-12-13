package org.apache.doris.udf.util;

import org.joda.time.DateTimeConstants;

public class DateUtils {

    public static int getIntDayOfWeek(String dayOfWeek) {
        if (dayOfWeek == null) {
            return -1;
        }
        if (DayOfWeek.MON.matches(dayOfWeek)) {
            return DateTimeConstants.MONDAY;
        }
        if (DayOfWeek.TUE.matches(dayOfWeek)) {
            return DateTimeConstants.TUESDAY;
        }
        if (DayOfWeek.WED.matches(dayOfWeek)) {
            return DateTimeConstants.WEDNESDAY;
        }
        if (DayOfWeek.THU.matches(dayOfWeek)) {
            return DateTimeConstants.THURSDAY;
        }
        if (DayOfWeek.FRI.matches(dayOfWeek)) {
            return DateTimeConstants.FRIDAY;
        }
        if (DayOfWeek.SAT.matches(dayOfWeek)) {
            return DateTimeConstants.SATURDAY;
        }
        if (DayOfWeek.SUN.matches(dayOfWeek)) {
            return DateTimeConstants.SUNDAY;
        }
        return -1;
    }

    public enum DayOfWeek {
        MON("MO", "MON", "MONDAY"), TUE("TU", "TUE", "TUESDAY"), WED("WE", "WED", "WEDNESDAY"), THU(
                "TH", "THU", "THURSDAY"), FRI("FR", "FRI", "FRIDAY"), SAT("SA", "SAT", "SATURDAY"), SUN(
                "SU", "SUN", "SUNDAY");

        private final String name2;
        private final String name3;
        private final String fullName;

        private DayOfWeek(String name2, String name3, String fullName) {
            this.name2 = name2;
            this.name3 = name3;
            this.fullName = fullName;
        }

        public String getName2() {
            return name2;
        }

        public String getName3() {
            return name3;
        }

        public String getFullName() {
            return fullName;
        }

        public boolean matches(String dayOfWeek) {
            if (dayOfWeek.length() == 2) {
                return name2.equalsIgnoreCase(dayOfWeek);
            }
            if (dayOfWeek.length() == 3) {
                return name3.equalsIgnoreCase(dayOfWeek);
            }
            return fullName.equalsIgnoreCase(dayOfWeek);
        }
    }

}
