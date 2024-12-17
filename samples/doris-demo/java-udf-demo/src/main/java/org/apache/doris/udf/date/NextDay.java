package org.apache.doris.udf.date;

import org.apache.hive.common.util.DateUtils;
import org.joda.time.DateTime;

import java.util.Date;

import static org.apache.doris.udf.util.DateUtils.getIntDayOfWeek;

/**
 * next_day
 */
public class NextDay {

    /**
     * next_day(varchar, varchar)
     * SELECT next_day('2015-01-14', 'TU');
     * @param inputTimestamp
     * @param dayOfWeek
     * @return
     */
    public String evaluate(String inputTimestamp, String dayOfWeek) {
        Date date;
        try {
            date = DateUtils.getDateFormat().parse(inputTimestamp);
        } catch (Exception e) {
            return null;
        }

        DateTime jodaTime = new DateTime(date);
        int currDayOfWeek = jodaTime.getDayOfWeek();
        int dayOfWeekInt = getIntDayOfWeek(dayOfWeek);
        if (dayOfWeekInt == -1) {
            return null;
        }

        int daysToAdd;
        if (currDayOfWeek < dayOfWeekInt) {
            daysToAdd = dayOfWeekInt - currDayOfWeek;
        } else {
            daysToAdd = 7 - currDayOfWeek + dayOfWeekInt;
        }

        DateTime newJodaTime = jodaTime.plusDays(daysToAdd);
        return DateUtils.getDateFormat().format(newJodaTime.toDate());
    }
}
