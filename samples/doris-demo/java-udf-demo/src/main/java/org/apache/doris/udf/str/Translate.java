package org.apache.doris.udf.str;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.udf.util.TranslateUtils;

/**
 * translate
 */
public class Translate {

    /**
     * translate(String, String, String)
     *
     * @param input
     * @param from
     * @param to
     * @return
     */
    public String evaluate(String input, String from, String to) {
        if (StringUtils.isEmpty(input) || StringUtils.isEmpty(from) || StringUtils.isEmpty(to)) {
            return null;
        }

        TranslateUtils.populateMappingsIfNecessary(from, to);
        return TranslateUtils.processInput(input);
    }
}
