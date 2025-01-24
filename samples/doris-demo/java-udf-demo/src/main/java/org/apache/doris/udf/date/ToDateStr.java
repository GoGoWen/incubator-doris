package org.apache.doris.udf.date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;


/**
 * to_date_str
 */
public class ToDateStr extends UDF {

    /**
     * to_date_str(String)
     * select to_date_str('2023-10-05 14:30:45');
     *
     * @param dateStr
     * @return
     */
    public String evaluate(String dateStr) {
        if (StringUtils.isEmpty(dateStr)) {
            return null;
        }

        if (dateStr.contains("T")) {
            dateStr = dateStr.replace("T", " ");
        }
        if (dateStr.contains(".")) {
            dateStr = dateStr.substring(0, dateStr.indexOf("."));
        }
        if (dateStr.contains("+")) {
            dateStr = dateStr.substring(0, dateStr.indexOf("+"));
        }
        if (dateStr.contains("Z")) {
            dateStr = dateStr.replace("Z", "+00:00");
        }

        if (dateStr.length()>19) {
            dateStr = dateStr.substring(0, 19);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(dateStr, formatter);
        return date.toString();
    }

    /**
     * to_date_str(String, String)
     * select to_date_str('2023-10-05 14:30:45');
     *
     * @param dateStr
     * @param format
     * @return
     */
    public LocalDate evaluate(String dateStr, String format) {
        if (StringUtils.isEmpty(dateStr) || StringUtils.isEmpty(format)) {
            return null;
        }

        if (dateStr.contains("T")) {
            dateStr = dateStr.replace("T", " ");
        }
        if (dateStr.contains(".")) {
            dateStr = dateStr.substring(0, dateStr.indexOf("."));
        }
        if (dateStr.contains("+")) {
            dateStr = dateStr.substring(0, dateStr.indexOf("+"));
        }
        if (dateStr.contains("Z")) {
            dateStr = dateStr.replace("Z", "+00:00");
        }

        if (dateStr.length()>19) {
            dateStr = dateStr.substring(0, 19);
        }

        if (format.equalsIgnoreCase("yyyy-MM-dd")) {
            format = "yyyy-MM-dd";
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format).withLocale(Locale.getDefault());
        return LocalDate.parse(dateStr, formatter);
    }

    /**
     * to_date_str(Datetime)
     * SELECT to_date_str(cast('2024-12-12 03:04:05.321' as DATETIME));
     * select to_date_str(cast('2024-07-09 09:20:14.000000' as DATETIME));
     *
     * @param timestamp
     * @return
     */
    public String evaluate(Long timestamp) {
        if (timestamp == null) {
            return null;
        }

        return formatDatetime(timestamp);
    }

    private String formatDatetime(long timestamp) {
        try {
            return DateTimeFormat.forPattern("yyyy-MM-dd")
                .withLocale(Locale.getDefault())
                .print(timestamp);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("ToDate formatDatetime error ", e);
        }
    }
}
