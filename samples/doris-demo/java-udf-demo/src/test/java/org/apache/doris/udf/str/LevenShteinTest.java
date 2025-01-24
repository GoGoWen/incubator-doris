package org.apache.doris.udf.str;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LevenShteinTest {

    @Test
    public void testEvaluateBothStringsNotEmpty() {
        LevenShtein levenShtein = new LevenShtein();
        String string1 = "kitten";
        String string2 = "sitting";
        Long result = levenShtein.evaluate(string1, string2);
        assertEquals(Long.valueOf(3), result);
    }

    @Test
    public void testEvaluateString1Empty() {
        LevenShtein levenShtein = new LevenShtein();
        String string1 = "";
        String string2 = "sitting";
        Long result = levenShtein.evaluate(string1, string2);
        assertNull(result);
    }

    @Test
    public void testEvaluateString2Empty() {
        LevenShtein levenShtein = new LevenShtein();
        String string1 = "kitten";
        String string2 = "";
        Long result = levenShtein.evaluate(string1, string2);
        assertNull(result);
    }

    @Test
    public void testEvaluateBothStringsEmpty() {
        LevenShtein levenShtein = new LevenShtein();
        String string1 = "";
        String string2 = "";
        Long result = levenShtein.evaluate(string1, string2);
        assertNull(result);
    }

}
