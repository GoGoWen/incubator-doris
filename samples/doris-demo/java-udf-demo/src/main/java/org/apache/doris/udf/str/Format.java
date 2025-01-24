package org.apache.doris.udf.str;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Date;
import java.util.IllegalFormatException;

/**
 * format
 */
public class Format extends UDF {

    /**
     * format(String, String)
     * format.evaluate("%s%%", 123);
     *
     * @param format
     * @param arg
     * @return
     */
    public String evaluate(String format, String arg) {
        if (StringUtils.isEmpty(format) || arg == null) {
            return null;
        }

        return format(format, arg);
    }

    public String evaluate(String format, String arg, String arg1) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null) {
            return null;
        }

        return format(format, arg, arg1);
    }

    public String evaluate(String format, String arg, String arg1, String arg2) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null || arg3 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3, String arg4) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null || arg3 == null || arg4 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3, arg4);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3,
                           String arg4, String arg5) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null
            || arg3 == null || arg4 == null || arg5 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3, arg4, arg5);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3,
                           String arg4, String arg5, String arg6) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null
            || arg3 == null || arg4 == null || arg5 == null || arg6 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3,
                           String arg4, String arg5, String arg6, String arg7) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null
            || arg3 == null || arg4 == null || arg5 == null || arg6 == null || arg7 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3,
                           String arg4, String arg5, String arg6, String arg7, String arg8) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null || arg3 == null
            || arg4 == null || arg5 == null || arg6 == null || arg7 == null || arg8 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
    }

    public String evaluate(String format, String arg, String arg1, String arg2, String arg3, String arg4,
                           String arg5, String arg6, String arg7, String arg8, String arg9) {
        if (StringUtils.isEmpty(format) || arg == null || arg1 == null || arg2 == null || arg3 == null
            || arg4 == null || arg5 == null || arg6 == null || arg7 == null || arg8 == null || arg9 == null) {
            return null;
        }

        return format(format, arg, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
    }

    /**
     * format(String, float)
     * format.evaluate("%s%%", 123);
     *
     * @param format
     * @param arg
     * @return
     */
    public String evaluate(String format, Float arg) {
        if (StringUtils.isEmpty(format) || arg == null) {
            return null;
        }

        return format(format, arg);
    }

    /**
     * format(String, int)
     * format.evaluate("%s%%", 123);
     *
     * @param format
     * @param arg
     * @return
     */
    public String evaluate(String format, Integer arg) {
        if (StringUtils.isEmpty(format) || arg == null) {
            return null;
        }

        return format(format, arg);
    }

    /**
     * format(String, double)
     * format.evaluate("%s%%", 123...);
     *
     * @param format
     * @param arg
     * @return
     */
    public String evaluate(String format, Double arg) {
        if (StringUtils.isEmpty(format) || arg == null) {
            return null;
        }

        return format(format, arg);
    }

    /**
     * format(String, datetime)
     * SELECT format('%1$tA, %1$tB %1$te, %1$tY', Datetime '2006-07-04');
     *
     * @param format
     * @param arg
     * @return
     */
    public String evaluate(String format, Date arg) {
        if (StringUtils.isEmpty(format) || arg == null) {
            return null;
        }

        return format(format, arg);
    }

    public String format(String formatStr, Object... values) {
        try {
            return String.format(formatStr, values);
        } catch (IllegalFormatException e) {
            throw new IllegalArgumentException("Invalid format string or parameter mismatch.");
        }
    }
}
