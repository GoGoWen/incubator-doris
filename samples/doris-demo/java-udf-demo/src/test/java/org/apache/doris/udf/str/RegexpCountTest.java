package org.apache.doris.udf.str;

 import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class RegexpCountTest {

    @Test
    public void testEvaluateWhenTextAndRegexNotNullExpectCount() {
        RegexpCount regexpCount = new RegexpCount();
        String text = "apple orange apple banana apple";
        String regex = "apple";
        Long result = regexpCount.evaluate(text, regex);
        assertEquals(Long.valueOf(3), result);
    }

    @Test
    public void testEvaluateWhenTextIsNullExpectNull() {
        RegexpCount regexpCount = new RegexpCount();
        String text = null;
        String regex = "apple";
        Long result = regexpCount.evaluate(text, regex);
        assertNull(result);
    }

    @Test
    public void testEvaluateWhenRegexIsNullExpectNull() {
        RegexpCount regexpCount = new RegexpCount();
        String text = "apple orange apple banana apple";
        String regex = null;
        Long result = regexpCount.evaluate(text, regex);
        assertNull(result);
    }

    @Test
    public void testEvaluateWhenTextAndRegexAreEmptyExpectZero() {
        RegexpCount regexpCount = new RegexpCount();
        String text = "";
        String regex = "";
        Long result = regexpCount.evaluate(text, regex);
        assertEquals(Long.valueOf(1), result);
    }

    @Test
    public void testEvaluateWhenTextContainsNoMatchExpectZero() {
        RegexpCount regexpCount = new RegexpCount();
        String text = "orange banana";
        String regex = "apple";
        Long result = regexpCount.evaluate(text, regex);
        assertEquals(Long.valueOf(0), result);
    }

    @Test
    public void testEvaluateWhenTextContainsSingleMatchExpectOne() {
        RegexpCount regexpCount = new RegexpCount();
        String text = "apple";
        String regex = "apple";
        Long result = regexpCount.evaluate(text, regex);
        assertEquals(Long.valueOf(1), result);
    }

    @Test
    public void testEvaluateWhenTextContainsMultipleMatchesExpectCount() {
        RegexpCount regexpCount = new RegexpCount();
        String text = "apple apple apple";
        String regex = "apple";
        Long result = regexpCount.evaluate(text, regex);
        assertEquals(Long.valueOf(3), result);
    }

}
