package org.apache.doris.udf.str;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * levenshtein
 */
public class LevenShtein extends UDF {

    /**
     * levenshtein(String, String)
     * @param string1
     * @param string2
     * @return
     */
    public Long evaluate(String string1, String string2) {
        if(StringUtils.isEmpty(string1) || StringUtils.isEmpty(string2)) {
            return null;
        }

        int dist = StringUtils.getLevenshteinDistance(string1, string2);
        return Long.parseLong(String.valueOf(dist));
    }
}
