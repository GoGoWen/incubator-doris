package org.apache.doris.udf.date;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.udf.util.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * date_format
 */
public class DateFormat extends UDF {

    /**
     * date_fmt(String, String)
     *
     * @param dt
     * @param fmt
     * @return
     */
    public String evaluate(String dt, String fmt) {
        if(StringUtils.isEmpty(dt) || StringUtils.isEmpty(fmt)) {
            return null;
        }

        Date date = DateUtils.getTimestampFromString(dt);
        SimpleDateFormat sdf = DateUtils.getDateFormat(fmt);
        if (date == null || sdf == null) {
            return null;
        }
        return sdf.format(date);
    }
}
