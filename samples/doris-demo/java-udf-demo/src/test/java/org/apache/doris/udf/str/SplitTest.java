package org.apache.doris.udf.str;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SplitTest {

    private Split split;

    @BeforeEach
    public void setUp() {
        split = new Split();
    }

    @Test
    public void testEvaluateWithValidInput() {
        ArrayList<String> result = split.evaluate("apple,orange,banana", ",");
        assertEquals(3, result.size());
    }

    @Test
    public void testEvaluateWithNullString() {
        assertNull(split.evaluate(null, ","));
    }

    @Test
    public void testEvaluateWithNullDelimiter() {
        assertNull(split.evaluate("apple,orange,banana", null));
    }

    @Test
    public void testEvaluateWithEmptyString() {
        ArrayList<String> result = split.evaluate("", ",");
        assertEquals(1, result.size());
    }

    @Test
    public void testEvaluateWithSingleElement() {
        ArrayList<String> result = split.evaluate("apple", ",");
        assertEquals(1, result.size());
    }

    @Test
    public void testEvaluateWithMultipleDelimiters() {
        ArrayList<String> result = split.evaluate("apple,,orange,,banana", ",,");
        assertEquals(3, result.size());
    }

    @Test
    public void testEvaluateWithNoDelimiterInString() {
        ArrayList<String> result = split.evaluate("appleorangebanana", ",");
        assertEquals(1, result.size());
    }

    @Test
    public void testBasicSplit() {
        ArrayList<String> result = split.evaluate("a,b,c", ",", 10);
        assert Arrays.asList("a", "b", "c").equals(result) : "Basic split test failed";
    }

    @Test
    public void testSplitWithLimit() {
        setUp();
        ArrayList<String> result = split.evaluate("a,b,c,d,e", ",", 3);
        assert Arrays.asList("a", "b", "c,d,e").equals(result) : "Limit parameter test failed";
    }

    @Test
    public void testSplitWithLimitOne() {
        setUp();
        ArrayList<String> result = split.evaluate("a,b,c", ",", 1);
        assert Arrays.asList("a,b,c").equals(result) : "Limit=1 test failed";
    }

    @Test
    public void testMultiCharDelimiter() {
        setUp();
        ArrayList<String> result = split.evaluate("a::b::c", "::", 10);
        assert Arrays.asList("a", "b", "c").equals(result) : "Multi-character delimiter test failed";
    }

    @Test
    public void testChineseStringSplit() {
        setUp();
        ArrayList<String> result = split.evaluate("Beijing-Shanghai-Guangzhou", "-", 10);
        assert Arrays.asList("Beijing", "Shanghai", "Guangzhou").equals(result) : "Chinese string split test failed";
    }

    @Test
    public void testEmptyString() {
        setUp();
        ArrayList<String> result = split.evaluate("", ",", 10);
        assert Arrays.asList("").equals(result) : "Empty string test failed";
    }

    @Test
    public void testNullString() {
        setUp();
        ArrayList<String> result = split.evaluate(null, ",", 10);
        assert result == null : "Null string test failed";
    }

    @Test
    public void testDelimiterNotFound() {
        setUp();
        ArrayList<String> result = split.evaluate("abc", ",", 10);
        assert Arrays.asList("abc").equals(result) : "Delimiter not found test failed";
    }

    @Test
    public void testStringStartsWithDelimiter() {
        setUp();
        ArrayList<String> result = split.evaluate(",a,b", ",", 10);
        assert Arrays.asList("", "a", "b").equals(result) : "String starts with delimiter test failed";
    }

    @Test
    public void testStringEndsWithDelimiter() {
        setUp();
        ArrayList<String> result = split.evaluate("a,b,", ",", 10);
        assert Arrays.asList("a", "b", "").equals(result) : "String ends with delimiter test failed";
    }

    @Test
    public void testConsecutiveDelimiters() {
        setUp();
        ArrayList<String> result = split.evaluate("a,,b", ",", 10);
        assert Arrays.asList("a", "", "b").equals(result) : "Consecutive delimiters test failed";
    }

    @Test
    public void testOnlyDelimiters() {
        setUp();
        ArrayList<String> result = split.evaluate(",,,", ",", 10);
        assert Arrays.asList("", "", "", "").equals(result) : "Only delimiters string test failed";
    }

    @Test
    public void testNullDelimiter() {
        setUp();
        try {
            split.evaluate("a,b,c", null, 10);
            assert false : "Null delimiter should throw exception";
        } catch (IllegalArgumentException e) {
            assert "Delimiter cannot be null".equals(e.getMessage()) : "Exception message incorrect";
        }
    }

    @Test
    public void testEmptyDelimiter() {
        setUp();
        try {
            split.evaluate("a,b,c", "", 10);
            assert false : "Empty delimiter should throw exception";
        } catch (IllegalArgumentException e) {
            assert "Delimiter cannot be empty".equals(e.getMessage()) : "Exception message incorrect";
        }
    }

    @Test
    public void testNullLimit() {
        setUp();
        try {
            split.evaluate("a,b,c", ",", null);
            assert false : "Null limit should throw exception";
        } catch (IllegalArgumentException e) {
            assert "Limit parameter cannot be null".equals(e.getMessage()) : "Exception message incorrect";
        }
    }

    @Test
    public void testZeroLimit() {
        setUp();
        try {
            split.evaluate("a,b,c", ",", 0);
            assert false : "Zero limit should throw exception";
        } catch (IllegalArgumentException e) {
            assert "Limit must be positive, current value: 0".equals(e.getMessage()) : "Exception message incorrect";
        }
    }

    @Test
    public void testNegativeLimit() {
        setUp();
        try {
            split.evaluate("a,b,c", ",", -1);
            assert false : "Negative limit should throw exception";
        } catch (IllegalArgumentException e) {
            assert "Limit must be positive, current value: -1".equals(e.getMessage()) : "Exception message incorrect";
        }
    }
}
