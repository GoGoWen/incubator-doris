package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.common.util.DateUtils;
import org.joda.time.DateTime;

import java.util.Date;

/**
 * Trunc
 */
public class Trunc extends UDF {

    /**
     * trunc(varchar, varchar)
     * SELECT trunc('2015-03-17', 'MM');
     * @param inputTimestamp
     * @param fmtInput
     * @return
     */
    public String evaluate(String inputTimestamp, String fmtInput) {
        Date date;
        try {
            date = DateUtils.getDateFormat().parse(inputTimestamp);
        } catch (Exception e) {
            return null;
        }
        DateTime jodaTime = new DateTime(date);
        DateTime newTime = null;
        if ("MONTH".equals(fmtInput) || "MON".equals(fmtInput) || "MM".equals(fmtInput)) {
            newTime = jodaTime.dayOfMonth().withMinimumValue();
        } else if ("QUARTER".equals(fmtInput) || "Q".equals(fmtInput)) {
            int month = jodaTime.getMonthOfYear() - 1;
            int quarter = month / 3;
            int monthToSet = quarter * 3;
            newTime = jodaTime.monthOfYear().setCopy(monthToSet + 1).dayOfMonth().setCopy(1);
        } else if ("YEAR".equals(fmtInput) || "YYYY".equals(fmtInput) || "YY".equals(fmtInput)) {
            newTime = jodaTime.dayOfMonth().withMinimumValue().monthOfYear().withMinimumValue();
        }

        if (newTime == null) {
            return null;
        }
        return DateUtils.getDateFormat().format(newTime.toDate());
    }
}
