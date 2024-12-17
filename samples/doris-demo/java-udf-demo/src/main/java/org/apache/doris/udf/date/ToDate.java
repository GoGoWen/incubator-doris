package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;


/**
 * to_date
 */
public class ToDate extends UDF {

    /**
     * to_date(varchar)
     * select select to_date('2023-10-05 14:30:45');
     * @param inputTimestamp
     * @return
     */
    public String evaluate(String inputTimestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(inputTimestamp, formatter);
        return date.toString();
    }

    /**
     * to_date(timestamp(p))
     * SELECT to_date(cast('2024-12-12 03:04:05.321' as DATETIME));
     * select to_date(cast('2024-07-09 09:20:14.000000' as DATETIME));
     * @param timestamp
     * @return
     */
    public String evaluate(long timestamp) {
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
