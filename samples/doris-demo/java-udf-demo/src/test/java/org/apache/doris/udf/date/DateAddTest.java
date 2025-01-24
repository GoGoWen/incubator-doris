package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Date;

public class DateAddTest {
    private DateAdd dateAdd;

    @Before
    public void setUp() {
        dateAdd = new DateAdd();
    }

    @Test
    public void testEvaluateStringDate() {
        // Test with date string format
        Assert.assertEquals("2023-10-06", dateAdd.evaluate("2023-10-05", 1));
        Assert.assertEquals("2023-10-04", dateAdd.evaluate("2023-10-05", -1));
        
        // Test with datetime string format
        Assert.assertEquals("2023-10-06 14:30:45", dateAdd.evaluate("2023-10-05 14:30:45", 1));
        Assert.assertEquals("2023-10-04 14:30:45", dateAdd.evaluate("2023-10-05 14:30:45", -1));
        
        // Test with invalid date format
        Assert.assertNull(dateAdd.evaluate("invalid-date", 1));
    }

    @Test
    public void testEvaluateWithUnitAndDate() {
        Date testDate = java.sql.Date.valueOf("2023-10-05");
        
        // Test different units
        // Note: The DateAdd class uses simplified calculations:
        // YEAR = 365 days
        // MONTH = 30 days
        Assert.assertEquals("2024-10-04", dateAdd.evaluate("YEAR", 1, testDate));
        Assert.assertEquals("2023-11-04", dateAdd.evaluate("MONTH", 1, testDate));
        Assert.assertEquals("2023-10-12", dateAdd.evaluate("WEEK", 1, testDate));
        Assert.assertEquals("2023-10-06", dateAdd.evaluate("DAY", 1, testDate));
        
        // Test negative values
        Assert.assertEquals("2022-10-05", dateAdd.evaluate("YEAR", -1, testDate));
        Assert.assertEquals("2023-09-05", dateAdd.evaluate("MONTH", -1, testDate));
        
        // Test hour, minute, second
        Assert.assertEquals("2023-10-06", dateAdd.evaluate("HOUR", 24, testDate));
        Assert.assertEquals("2023-10-06", dateAdd.evaluate("MINUTE", 1440, testDate));
        Assert.assertEquals("2023-10-06", dateAdd.evaluate("SECOND", 86400, testDate));
        
        // Test null inputs
        Assert.assertNull(dateAdd.evaluate(null, 1, testDate));
        Assert.assertNull(dateAdd.evaluate("YEAR", null, testDate));
        Assert.assertNull(dateAdd.evaluate("YEAR", 1, null));
    }

    @Test
    public void testEvaluateWithLocalDateTime() {
        LocalDateTime testDateTime = LocalDateTime.parse("2023-10-05T14:30:45");
        
        // Test positive days
        Assert.assertEquals("2023-10-06 14:30:45", dateAdd.evaluate(testDateTime, 1));
        Assert.assertEquals("2023-10-12 14:30:45", dateAdd.evaluate(testDateTime, 7));
        
        // Test negative days
        Assert.assertEquals("2023-10-04 14:30:45", dateAdd.evaluate(testDateTime, -1));
        Assert.assertEquals("2023-09-28 14:30:45", dateAdd.evaluate(testDateTime, -7));
        
        // Test null inputs
        Assert.assertNull(dateAdd.evaluate((LocalDateTime)null, 1));
        Assert.assertNull(dateAdd.evaluate(testDateTime, null));
    }
}
