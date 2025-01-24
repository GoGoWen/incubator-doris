package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.LocalDateTime;


/**
 * dow
 */
public class DayOfWeek extends UDF {

    /**
     * dow(Datetime)
     *
     * @param date
     * @return
     */
    public Double evaluate(LocalDateTime date) {
        if (date == null) {
            return null;
        }

        return (double) date.dayOfWeek().get();
    }
}
