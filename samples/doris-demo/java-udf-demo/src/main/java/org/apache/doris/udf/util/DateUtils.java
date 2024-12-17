package org.apache.doris.udf.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class DateUtils {

    private static Map<String, ThreadLocal<SimpleDateFormat>> dfMap = new HashMap<>();
    private static final Object lockObj = new Object();

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

    public static Timestamp getTimestampFromString(String s) {
        Timestamp result;
        s = s.trim();

        // Throw away extra if more than 9 decimal places
        int periodIdx = s.indexOf(".");
        if (periodIdx != -1) {
            if (s.length() - periodIdx > 9) {
                s = s.substring(0, periodIdx + 10);
            }
        }
        if (s.indexOf(' ') < 0) {
            s = s.concat(" 00:00:00");
        }
        try {
            result = Timestamp.valueOf(s);
        } catch (IllegalArgumentException e) {
            result = null;
        }
        return result;
    }

    public static int getDayPartInSec(DateTime dt) {
        int dd = dt.getDayOfMonth();
        int HH = dt.getHourOfDay();
        int mm = dt.getMinuteOfHour();
        int ss = dt.getSecondOfMinute();
        return dd * 86400 + HH * 3600 + mm * 60 + ss;
    }

    public static SimpleDateFormat getDateFormat(String pattern) {
        try {
            return getDfByPattern(pattern);
        } catch (Exception e) {
            return null;
        }
    }

    private static SimpleDateFormat getDfByPattern(String pattern) {
        ThreadLocal<SimpleDateFormat> df = dfMap.get(pattern);
        if (df == null) {
            synchronized (lockObj) {
                df = dfMap.get(pattern);
                if (df == null) {
                    df = new ThreadLocal<SimpleDateFormat>() {
                        @Override
                        protected SimpleDateFormat initialValue() {

                            return new SimpleDateFormat(pattern);
                        }

                    };
                    dfMap.put(pattern, df);
                }
            }
        }
        return df.get();
    }

}
