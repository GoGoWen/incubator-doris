package org.apache.doris.udf.num;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FormatNumberTest {

    @Test
    public void testEvaluateWithValidInputs() {
        FormatNumber formatNumber = new FormatNumber();
        String result = formatNumber.evaluate(123.456, 2);
        assertEquals("123.46", result);
    }

    @Test
    public void testEvaluateWithNullNum() {
        FormatNumber formatNumber = new FormatNumber();
        String result = formatNumber.evaluate(null, 2);
        assertNull(result);
    }

    @Test
    public void testEvaluateWithNullDecimalPlaces() {
        FormatNumber formatNumber = new FormatNumber();
        String result = formatNumber.evaluate(123.456, null);
        assertNull(result);
    }

    @Test
    public void testEvaluateWithNegativeNumAndDecimalPlaces() {
        FormatNumber formatNumber = new FormatNumber();
        String result = formatNumber.evaluate(-123.456, 2);
        assertEquals("-123.46", result);
    }

}
