package org.apache.doris.udf.str;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * levenshtein
 */
public class LevenShtein extends UDF {

    /**
     * levenshtein(varchar, varchar)
     * @param string1
     * @param string2
     * @return
     */
    public long evaluate(String string1, String string2) {
        int dist = StringUtils.getLevenshteinDistance(string1, string2);
        return Long.parseLong(String.valueOf(dist));
    }
}
