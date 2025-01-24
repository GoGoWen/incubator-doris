package org.apache.doris.udf.str;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * regexp_count
 */
public class RegexpCount extends UDF {

    /**
     * regexp_count(string, pattern)
     * SELECT regexp_count('1a 2b 14m', '\s*[a-z]+\s*');
     * SELECT regexp_count('1a 2b 3c 4d', '[0-9]');
     * SELECT regexp_count('hello world hello presto', 'hello');
     * SELECT regexp_count('a  b   c', '\s+');
     *
     * @param text
     * @param regex
     * @return
     */
    public Long evaluate(String text, String regex) {
        if (text == null || regex == null) {
            return null;
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);

        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
