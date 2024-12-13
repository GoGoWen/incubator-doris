package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * date_supplement
 */
public class DateSupplement extends UDF {

    /**
     * select date_supplement(5);
     * select date_supplement('2023-10-05');
     * select date_supplement('2023-10-05 14:30:45');
     * select date_supplement('invalid-date');
     * select date_supplement('');
     * select date_supplement('1');
     * select date_supplement('asdasdjsada');
     * @param inputDate
     * @return
     */
    public String evaluate(String inputDate) {
        // 定义日期格式化器
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try {
            // 尝试将输入解析为 LocalDate
            LocalDate localDate = LocalDate.parse(inputDate, dateFormatter);
            // 如果解析成功，补齐时分秒为 00:00:00
            LocalDateTime localDateTime = localDate.atStartOfDay();
            return localDateTime.format(dateTimeFormatter);
        } catch (DateTimeParseException e) {
            // 如果解析失败，尝试将输入解析为 LocalDateTime
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(inputDate, dateTimeFormatter);
                return localDateTime.format(dateTimeFormatter);
            } catch (DateTimeParseException e2) {
                return inputDate;
            }
        }
    }
}
