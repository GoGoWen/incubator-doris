package org.apache.doris.udf.str;

import org.apache.doris.udf.util.TranslateUtils;

/**
 * translate
 */
public class Translate {

    /**
     * translate(varchar, varchar, varchar)
     *
     * @param input
     * @param from
     * @param to
     * @return
     */
    public String evaluate(String input, String from, String to) {
        TranslateUtils.populateMappingsIfNecessary(from, to);
        return TranslateUtils.processInput(input);
    }
}
