package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.LocalDateTime;


/**
 * dow
 */
public class DayOfWeek extends UDF {

    /**
     * dow(Date)
     *
     * @param date
     * @return
     */
    public double evaluate(LocalDateTime date) {
        return date.dayOfWeek().get();
    }
}
