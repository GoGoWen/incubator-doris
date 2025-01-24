package org.apache.doris.udf.num;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DecimalFormat;

/**
 * format_number
 */
public class FormatNumber extends UDF {

    /**
     * format_number(number x, int d)
     * select format_number(12332.123456, 4)
     *
     * @param num
     * @param decimalPlaces
     * @return
     */
    public String evaluate(Double num, Integer decimalPlaces) {
        if (num == null || decimalPlaces == null) {
            return null;
        }

        StringBuilder pattern = new StringBuilder("0.");
        for (int i = 0; i < decimalPlaces; i++) {
            pattern.append("0");
        }

        DecimalFormat dFormat = new DecimalFormat(pattern.toString());
        return dFormat.format(num);
    }
}
