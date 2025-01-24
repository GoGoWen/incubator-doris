package org.apache.doris.udf.date;


public class Interval {
    private final long years;
    private final long months;
    private final long days;
    private final long hours;
    private final long minutes;
    private final long seconds;

    public Interval(long years, long months, long days, long hours, long minutes, long seconds) {
        this.years = years;
        this.months = months;
        this.days = days;
        this.hours = hours;
        this.minutes = minutes;
        this.seconds = seconds;
    }

    public long getYears() {
        return years;
    }

    public long getMonths() {
        return months;
    }

    public long getDays() {
        return days;
    }

    public long getHours() {
        return hours;
    }

    public long getMinutes() {
        return minutes;
    }

    public long getSeconds() {
        return seconds;
    }

    public static Interval parseInterval(String intervalStr) {
        String[] parts = intervalStr.split(" ");
        String[] dateParts = parts[0].split("-");
        String[] timeParts = parts[1].split(":");

        long years = Long.parseLong(dateParts[0]);
        long months = Long.parseLong(dateParts[1]);
        long days = Long.parseLong(dateParts[2]);
        long hours = Long.parseLong(timeParts[0]);
        long minutes = Long.parseLong(timeParts[1]);
        long seconds = Long.parseLong(timeParts[2]);

        return new Interval(years, months, days, hours, minutes, seconds);
    }
}
