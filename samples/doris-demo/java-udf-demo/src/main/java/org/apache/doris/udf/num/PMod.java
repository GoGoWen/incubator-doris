package org.apache.doris.udf.num;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * pmod
 */
public class PMod extends UDF {

    /**
     * pmod(DOUBLE a, DOUBLE b)
     * SELECT pmod(10, 3.1)
     *
     * @param num1
     * @param num2
     * @return
     */
    public Double evaluate(Double num1, Double num2) {
        if (num1 == null || num2 == null) {
            return null;
        }
        return ((num1 % num2) + num2) % num2;
    }
}
