package org.apache.doris.udf.str;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;


public class TranslateTest {

    @Test
    public void testEvaluateWithNullInputReturnsNull() {
        Translate translate = new Translate();
        assertNull(translate.evaluate(null, "from", "to"));
    }

    @Test
    public void testEvaluateWithEmptyInputReturnsNull() {
        Translate translate = new Translate();
        assertNull(translate.evaluate("", "from", "to"));
    }

    @Test
    public void testEvaluateWithEmptyFromReturnsNull() {
        Translate translate = new Translate();
        assertNull(translate.evaluate("input", "", "to"));
    }

    @Test
    public void testEvaluateWithEmptyToReturnsNull() {
        Translate translate = new Translate();
        assertNull(translate.evaluate("input", "from", ""));
    }

}
