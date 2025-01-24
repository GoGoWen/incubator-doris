package org.apache.doris.udf.date;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.udf.util.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * date_add_str
 */
public class DateAdd extends UDF {

    private static final Map<String, BiFunction<Date, Integer, Date>> TIME_UNIT_MAP = new HashMap<>();

    static {
        TIME_UNIT_MAP.put("YEAR", (date, value) -> new Date(date.getTime() + value * 365L * 24 * 60 * 60 * 1000));
        TIME_UNIT_MAP.put("MONTH", (date, value) -> new Date(date.getTime() + value * 30L * 24 * 60 * 60 * 1000));
        TIME_UNIT_MAP.put("WEEK", (date, value) -> new Date(date.getTime() + value * 7L * 24 * 60 * 60 * 1000));
        TIME_UNIT_MAP.put("DAY", (date, value) -> new Date(date.getTime() + value * 24L * 60 * 60 * 1000));
        TIME_UNIT_MAP.put("HOUR", (date, value) -> new Date(date.getTime() + value * 60L * 60 * 1000));
        TIME_UNIT_MAP.put("MINUTE", (date, value) -> new Date(date.getTime() + value * 60L * 1000));
        TIME_UNIT_MAP.put("SECOND", (date, value) -> new Date(date.getTime() + value * 1000L));
    }

    /**
     * date_add_str(String, int);
     * select date_add_str('2023-10-05',1);
     * select date_add_str('2023-10-05 14:30:45',-1);
     *
     * @param dateStr
     * @param days
     * @return
     */
    public String evaluate(String dateStr, Integer days) {
        try {
            LocalDate date = LocalDate.parse(dateStr, DateUtils.DATE_FORMATTER);
            LocalDate localDate = date.plusDays(days);
            return localDate.format(DateUtils.DATE_FORMATTER);
        } catch (Exception e) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(dateStr, DateUtils.DATE_TIME_FORMATTER);
                LocalDateTime localDateTime = dateTime.plusDays(days);
                return localDateTime.format(DateUtils.DATE_TIME_FORMATTER);
            } catch (Exception e1) {
                return null;
            }
        }
    }

    /**
     *
     * date_add_str(String, int, String);
     * select date_add_str('2023-10-05 14:30:45',-1);
     * @param unit
     * @param value
     * @param timestamp
     * @return
     */
    /*public String evaluate(String unit, Integer value, String timestamp) {
        if (StringUtils.isEmpty(unit) || value == null || StringUtils.isEmpty(timestamp)) {
            return null;
        }
        SimpleDateFormat sdf = timestamp.length() > 10 ? new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") : new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date date = sdf.parse(timestamp);
            Date newDate = addTime(date, unit.toUpperCase(), value);
            return new SimpleDateFormat("yyyy-MM-dd").format(newDate);
        } catch (ParseException e) {
            throw new RuntimeException("Invalid timestamp format", e);
        }
    }*/

    /**
     *
     * date_add_str(String, int, Date);
     * select date_add_str('week',-1,cast('2023-10-05 14:30:45' as date));
     * @param unit
     * @param value
     * @param date
     * @return
     */
    public String evaluate(String unit, Integer value, Date date) {
        if (StringUtils.isEmpty(unit) || value == null || date == null) {
            return null;
        }
        try {
            Date newDate = addTime(date, unit.toUpperCase(), value);
            return new SimpleDateFormat("yyyy-MM-dd").format(newDate);
        } catch (Exception e) {
            return null;
        }
    }

    private Date addTime(Date date, String unit, int value) {
        BiFunction<Date, Integer, Date> function = TIME_UNIT_MAP.get(unit);
        if (function == null) {
            throw new IllegalArgumentException("Invalid unit: " + unit);
        }
        return function.apply(date, value);
    }

    /**
     * date_add_str(Datetime, int);
     * select date_add_str(cast('2023-10-05 14:30:45' as datetime),-1);
     *
     * @param dateTime
     * @param days
     * @return
     */
    public String evaluate(LocalDateTime dateTime, Integer days) {
        if (dateTime == null || days == null) {
            return null;
        }

        try {
            LocalDateTime localDateTime = dateTime.plusDays(days);
            return localDateTime.format(DateUtils.DATE_TIME_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }
}
