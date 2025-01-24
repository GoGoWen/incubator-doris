package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DateFormatTest {
    private DateFormat dateFormat;

    @Before
    public void setUp() {
        dateFormat = new DateFormat();
    }

    @Test
    public void testEvaluateWithValidInput() {
        // Test date string with different formats
        Assert.assertEquals("2023-10-05", dateFormat.evaluate("2023-10-05", "yyyy-MM-dd"));
        Assert.assertEquals("2023/10/05", dateFormat.evaluate("2023-10-05", "yyyy/MM/dd"));
        Assert.assertEquals("05-10-2023", dateFormat.evaluate("2023-10-05", "dd-MM-yyyy"));
        Assert.assertEquals("Oct 05, 2023", dateFormat.evaluate("2023-10-05", "MMM dd, yyyy"));
        Assert.assertEquals("October 05, 2023", dateFormat.evaluate("2023-10-05", "MMMM dd, yyyy"));
        
        // Test datetime string with different formats
        Assert.assertEquals("2023-10-05 14:30:45", dateFormat.evaluate("2023-10-05 14:30:45", "yyyy-MM-dd HH:mm:ss"));
        Assert.assertEquals("14:30:45", dateFormat.evaluate("2023-10-05 14:30:45", "HH:mm:ss"));
        Assert.assertEquals("02:30 PM", dateFormat.evaluate("2023-10-05 14:30:45", "hh:mm a"));
        Assert.assertEquals("Thursday", dateFormat.evaluate("2023-10-05", "EEEE"));
        Assert.assertEquals("Thu", dateFormat.evaluate("2023-10-05", "EEE"));
    }

    @Test
    public void testEvaluateWithNullInput() {
        // Test null date string
        Assert.assertNull(dateFormat.evaluate(null, "yyyy-MM-dd"));
        
        // Test null format string
        Assert.assertNull(dateFormat.evaluate("2023-10-05", null));
        
        // Test both null
        Assert.assertNull(dateFormat.evaluate(null, null));
    }

    @Test
    public void testEvaluateWithEmptyInput() {
        // Test empty date string
        Assert.assertNull(dateFormat.evaluate("", "yyyy-MM-dd"));
        
        // Test empty format string
        Assert.assertNull(dateFormat.evaluate("2023-10-05", ""));
        
        // Test both empty
        Assert.assertNull(dateFormat.evaluate("", ""));
    }

    @Test
    public void testEvaluateWithInvalidInput() {
        // Test invalid date string
        Assert.assertNull(dateFormat.evaluate("invalid-date", "yyyy-MM-dd"));
        
        // Test invalid format string
        Assert.assertNull(dateFormat.evaluate("2023-10-05", "invalid-format"));
        
        // Test malformed date string
        Assert.assertNull(dateFormat.evaluate("2023-13-45", "yyyy-MM-dd"));
    }
}
