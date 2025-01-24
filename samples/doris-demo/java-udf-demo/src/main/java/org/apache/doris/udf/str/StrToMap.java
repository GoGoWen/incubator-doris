package org.apache.doris.udf.str;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Map;

/**
 * str_to_map
 */
public class StrToMap extends UDF {

    /**
     * str_to_map(String, String, String)
     * SELECT str_to_map('a:1,b:2,c:3', ',', ':')
     * @param input
     * @param entryDelimiter
     * @param keyValueDelimiter
     * @return
     */
    public String evaluate(String input, String entryDelimiter, String keyValueDelimiter) {
        if (StringUtils.isEmpty(input) || StringUtils.isEmpty(entryDelimiter)
            || StringUtils.isEmpty(keyValueDelimiter)) {
            return null;
        }

        if (input.isEmpty()) {
            return "{}";
        }

        isEmpty(entryDelimiter, "entryDelimiter");
        isEmpty(keyValueDelimiter, "keyValueDelimiter");
        if (keyValueDelimiter.equals(entryDelimiter)) {
            throw new RuntimeException("entryDelimiter and keyValueDelimiter must not be the same");
        }

        Map<String, String> resultMap = Maps.newHashMap();
        String[] entries = input.split(entryDelimiter);
        for (String entry : entries) {
            String[] keyValue = entry.split(keyValueDelimiter, 2);
            if (keyValue.length == 2) {
                resultMap.put(keyValue[0].trim(), keyValue[1].trim());
            } else if (keyValue.length == 1) {
                resultMap.put(keyValue[0].trim(), "");
            }
        }
        return resultMap.toString();
    }

    private static void isEmpty(String param, String errMsg) {
        if (param.isEmpty()) {
            throw new RuntimeException(errMsg + " is empty");
        }
    }
}
