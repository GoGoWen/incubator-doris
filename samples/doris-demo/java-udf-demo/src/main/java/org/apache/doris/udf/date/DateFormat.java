package org.apache.doris.udf.date;

import org.apache.doris.udf.util.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * date_format
 */
public class DateFormat extends UDF {

    /**
     * date_fmt(varchar, varchar)
     *
     * @param dt
     * @param fmt
     * @return
     */
    public String evaluate(String dt, String fmt) {
        Date date = DateUtils.getTimestampFromString(dt);
        SimpleDateFormat sdf = DateUtils.getDateFormat(fmt);
        if (date == null || sdf == null) {
            return null;
        }
        return sdf.format(date);
    }
}
