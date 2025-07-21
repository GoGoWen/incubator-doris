package org.apache.doris.udf.str;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StrToMapTest {

    @Test
    public void testEvaluateInputEmptyReturnNull() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate("", ",", ":");
        assertNull(evaluate);
    }

    @Test
    public void testEvaluateEntryDelimiterEmptyReturnNull() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate("a:1,b:2", "", ":");
        assertNull(evaluate);
    }

    @Test
    public void testEvaluateKeyValueDelimiterEmptyReturnNull() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate("a:1,b:2", ",", "");
        assertNull(evaluate);
    }

    @Test
    public void testEvaluateInputNotEmptyReturnMapString() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate("a:1,b:2", ",", ":");
        assertEquals("{a=1, b=2}", evaluate.toString());
    }

    @Test
    public void testEvaluateEntryDelimiterEqualsKeyValueDelimiterThrowException() {
        StrToMap strToMap = new StrToMap();
        try {
            strToMap.evaluate("a:1,b:2", ":", ":");
        } catch (RuntimeException e) {
            assertEquals("entryDelimiter and keyValueDelimiter must not be the same", e.getMessage());
        }
    }

    @Test
    public void testEvaluateKeyValueDelimiterEqualsEntryDelimiterThrowException() {
        StrToMap strToMap = new StrToMap();
        try {
            strToMap.evaluate("a:1,b:2", ",", ",");
        } catch (RuntimeException e) {
            assertEquals("entryDelimiter and keyValueDelimiter must not be the same", e.getMessage());
        }
    }

    @Test
    public void testEvaluateKeyValueDelimiterNotEqualsEntryDelimiterReturnMapString() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate("a:1,b:2", ",", ":");
        assertEquals("{a=1, b=2}", evaluate.toString());
    }

    @Test
    public void testEvaluateKeyValueDelimiterNotEqualsEntryDelimiterWithEmptyValueReturnMapString() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate("a:,b:2", ",", ":");
        assertEquals("{a=, b=2}", evaluate.toString());
    }

    @Test
    public void testEvaluateKeyValueDelimiterNotEqualsEntryDelimiterWithTrimmedValuesReturnMapString() {
        StrToMap strToMap = new StrToMap();
        Map<String, String> evaluate = strToMap.evaluate(" a : 1 , b : 2 ", ",", ":");
        assertEquals("{a=1, b=2}", evaluate.toString());
    }

}
