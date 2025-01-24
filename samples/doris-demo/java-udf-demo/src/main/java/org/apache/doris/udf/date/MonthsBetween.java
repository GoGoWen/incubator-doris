package org.apache.doris.udf.date;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.udf.util.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;

import static java.math.BigDecimal.ROUND_HALF_UP;

/**
 * months_between
 */
public class MonthsBetween extends UDF {

    /**
     * months_between(String, String)
     * SELECT months_between('1997-02-28 10:30:00', '1996-10-30')
     * @param date1
     * @param date2
     * @return
     */
    public Double evaluate(String date1, String date2) {
        if(StringUtils.isEmpty(date1) || StringUtils.isEmpty(date2)) {
            return null;
        }

        Date d1 = DateUtils.getTimestampFromString(date1);
        Date d2 = DateUtils.getTimestampFromString(date2);
        if (d1 == null || d2 == null) {
            throw new RuntimeException("'" + date1 + "' or " + "'" + date2 + "'" + " is not a valid DATE field");
        }

        DateTime jodaTime1 = new DateTime(d1);
        DateTime jodaTime2 = new DateTime(d2);
        // skip day/time part if both dates are end of the month
        // or the same day of the month
        int monDiffInt = (jodaTime1.getYear() - jodaTime2.getYear()) * 12 +
                (jodaTime1.getMonthOfYear() - jodaTime2.getMonthOfYear());
        if (jodaTime1.getDayOfMonth() == jodaTime2.getDayOfMonth()
            || (jodaTime1.getDayOfMonth() == jodaTime1.dayOfMonth().withMaximumValue().getDayOfMonth() &&
            jodaTime2.getDayOfMonth() == jodaTime2.dayOfMonth().withMaximumValue().getDayOfMonth())) {
            return (double) monDiffInt;
        }

        int sec1 = DateUtils.getDayPartInSec(jodaTime1);
        int sec2 = DateUtils.getDayPartInSec(jodaTime2);

        // 1 sec is 0.000000373 months (1/2678400). 1 month is 31 days.
        // there should be no adjustments for leap seconds
        double monBtwDbl = monDiffInt + (sec1 - sec2) / 2678400D;
        // Round a double to 8 decimal places.
        monBtwDbl = BigDecimal.valueOf(monBtwDbl).setScale(8, ROUND_HALF_UP).doubleValue();
        return monBtwDbl;
    }
}
