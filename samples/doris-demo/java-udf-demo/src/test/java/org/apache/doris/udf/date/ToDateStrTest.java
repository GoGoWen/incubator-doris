package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

public class ToDateStrTest {
    private ToDateStr toDateStr;

    @Before
    public void setUp() {
        toDateStr = new ToDateStr();
    }

    @Test
    public void testEvaluateWithSingleString() {
        // Test normal date-time string
        Assert.assertEquals("2023-10-05", toDateStr.evaluate("2023-10-05 14:30:45"));
        
        // Test date string without time
        Assert.assertEquals("2023-10-05", toDateStr.evaluate("2023-10-05"));
        
        // Test string with T separator
        Assert.assertEquals("2023-10-05", toDateStr.evaluate("2023-10-05T14:30:45"));
        
        // Test string with milliseconds
        Assert.assertEquals("2023-10-05", toDateStr.evaluate("2023-10-05 14:30:45.123"));
        
        // Test string with timezone
        Assert.assertEquals("2023-10-05", toDateStr.evaluate("2023-10-05 14:30:45+08:00"));
        
        // Test string with Z timezone
        Assert.assertEquals("2023-10-05", toDateStr.evaluate("2023-10-05T14:30:45Z"));
    }

    @Test
    public void testEvaluateWithStringAndFormat() {
        // Test with yyyy-MM-dd format for date only strings
        LocalDate expected = LocalDate.of(2023, 10, 5);
        Assert.assertEquals(expected, toDateStr.evaluate("2023-10-05", "yyyy-MM-dd"));
        
        // Test with explicit yyyy-MM-dd format
        Assert.assertEquals(expected, toDateStr.evaluate("2023-10-05", "YYYY-MM-dd"));
        
        // Test date-time string with format that includes time
        Assert.assertEquals(expected, toDateStr.evaluate("2023-10-05 14:30:45", "yyyy-MM-dd[ HH:mm:ss]"));
    }

    @Test
    public void testEvaluateWithTimestamp() {
        // Test with normal timestamp
        Assert.assertEquals("2024-12-13", toDateStr.evaluate(1734019445000L)); // 2024-12-12 03:04:05
        
        // Test with zero timestamp
        Assert.assertEquals("1970-01-01", toDateStr.evaluate(0L));
        
        // Test with current timestamp
        long currentTime = System.currentTimeMillis();
        Assert.assertNotNull(toDateStr.evaluate(currentTime));
    }

    @Test
    public void testNullAndEmptyInputs() {
        // Test null inputs for single string
        Assert.assertNull(toDateStr.evaluate((String)null));
        Assert.assertNull(toDateStr.evaluate(""));
        
        // Test null inputs for string and format
        Assert.assertNull(toDateStr.evaluate(null, "yyyy-MM-dd"));
        Assert.assertNull(toDateStr.evaluate("2023-10-05", null));
        Assert.assertNull(toDateStr.evaluate("", "yyyy-MM-dd"));
        Assert.assertNull(toDateStr.evaluate("2023-10-05", ""));
        
        // Test null timestamp
        Assert.assertNull(toDateStr.evaluate((Long)null));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidDateString() {
        toDateStr.evaluate("invalid-date");
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidDateFormat() {
        toDateStr.evaluate("2023-10-05", "invalid-format");
    }

    @Test
    public void testInvalidTimestamp() {
        // Test with negative timestamp
        Assert.assertNotNull(toDateStr.evaluate(-1L));
        
        // Test with very large timestamp
        Assert.assertNotNull(toDateStr.evaluate(Long.MAX_VALUE / 1000));
    }
}
