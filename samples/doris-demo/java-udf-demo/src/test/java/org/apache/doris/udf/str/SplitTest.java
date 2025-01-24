package org.apache.doris.udf.str;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SplitTest {

    @Test
    public void testEvaluateWithValidInput() {
        Split split = new Split();
        ArrayList<String> result = split.evaluate("apple,orange,banana", ",");
        assertEquals(3, result.size());
    }

    @Test
    public void testEvaluateWithNullString() {
        Split split = new Split();
        assertNull(split.evaluate(null, ","));
    }

    @Test
    public void testEvaluateWithNullDelimiter() {
        Split split = new Split();
        assertNull(split.evaluate("apple,orange,banana", null));
    }

    @Test
    public void testEvaluateWithEmptyString() {
        Split split = new Split();
        ArrayList<String> result = split.evaluate("", ",");
        assertEquals(1, result.size());
    }

    @Test
    public void testEvaluateWithSingleElement() {
        Split split = new Split();
        ArrayList<String> result = split.evaluate("apple", ",");
        assertEquals(1, result.size());
    }

    @Test
    public void testEvaluateWithMultipleDelimiters() {
        Split split = new Split();
        ArrayList<String> result = split.evaluate("apple,,orange,,banana", ",,");
        assertEquals(3, result.size());
    }

    @Test
    public void testEvaluateWithNoDelimiterInString() {
        Split split = new Split();
        ArrayList<String> result = split.evaluate("appleorangebanana", ",");
        assertEquals(1, result.size());
    }

}
