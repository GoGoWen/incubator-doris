package org.apache.doris.udf.date;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.udf.util.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;

/**
 * date_supplement
 */
public class DateSupplement extends UDF {

    /**
     * date_supplement(String)
     * select date_supplement('2023-10-05');
     * select date_supplement('2023-10-05 14:30:45');
     * select date_supplement('invalid-date');
     * select date_supplement(null);
     *
     * @param inputDate
     * @return
     */
    public String evaluate(String inputDate) {
        if (StringUtils.isEmpty(inputDate)) {
            return inputDate;
        }

        try {
            return formatDateTime(inputDate);
        } catch (DateTimeParseException e) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(inputDate, DateUtils.DATE_TIME_FORMATTER);
                return localDateTime.format(DateUtils.DATE_TIME_FORMATTER);
            } catch (DateTimeParseException e2) {
                return inputDate;
            }
        }
    }

    private String formatDateTime(String inputDate) {
        if (inputDate.contains("-")) {
            long dashCount = inputDate.chars().filter(c -> c == '-').count();
            LocalDate date = parseDate(inputDate, dashCount);
            LocalDateTime localDateTime = date.atStartOfDay();
            return localDateTime.format(DateUtils.DATE_TIME_FORMATTER);
        } else {
            Year year = Year.parse(inputDate, DateUtils.YEAR_MATTER);
            LocalDateTime firstDayOfYear = year.atDay(1).atStartOfDay();
            return firstDayOfYear.format(DateUtils.DATE_TIME_FORMATTER);
        }
    }

    private LocalDate parseDate(String inputDate, long dashCount) {
        if (dashCount == 1 && inputDate.length() == 7) {
            YearMonth yearMonth = YearMonth.parse(inputDate, DateUtils.YEAR_MONTH_MATTER);
            return yearMonth.atDay(1);
        } else {
            return LocalDate.parse(inputDate, DateUtils.DATE_FORMATTER);
        }
    }
}
