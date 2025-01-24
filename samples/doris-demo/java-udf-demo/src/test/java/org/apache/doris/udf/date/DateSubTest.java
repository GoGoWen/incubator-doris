package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Date;

public class DateSubTest {
    private DateSub dateSub;

    @Before
    public void setUp() {
        dateSub = new DateSub();
    }

    @Test
    public void testEvaluateStringDate() {
        // Test with date string format
        Assert.assertEquals("2023-10-04", dateSub.evaluate("2023-10-05", 1));
        Assert.assertEquals("2023-10-06", dateSub.evaluate("2023-10-05", -1));
        
        // Test with datetime string format
        Assert.assertEquals("2023-10-04 14:30:45", dateSub.evaluate("2023-10-05 14:30:45", 1));
        Assert.assertEquals("2023-10-06 14:30:45", dateSub.evaluate("2023-10-05 14:30:45", -1));
        
        // Test with invalid date format
        Assert.assertNull(dateSub.evaluate("invalid-date", 1));
        
        // Test with null inputs
        Assert.assertNull(dateSub.evaluate((String)null, 1));
        Assert.assertNull(dateSub.evaluate("2023-10-05", null));
    }

    @Test
    public void testEvaluateWithUnitAndDate() {
        Date testDate = java.sql.Date.valueOf("2023-10-05");
        
        // Test different units
        // Note: The DateSub class uses simplified calculations:
        // YEAR = 365 days
        // MONTH = 30 days
        Assert.assertEquals("2022-10-05", dateSub.evaluate("YEAR", 1, testDate));
        Assert.assertEquals("2023-09-05", dateSub.evaluate("MONTH", 1, testDate));
        Assert.assertEquals("2023-09-28", dateSub.evaluate("WEEK", 1, testDate));
        Assert.assertEquals("2023-10-04", dateSub.evaluate("DAY", 1, testDate));
        
        // Test negative values (which effectively become additions)
        Assert.assertEquals("2024-10-04", dateSub.evaluate("YEAR", -1, testDate));
        Assert.assertEquals("2023-11-04", dateSub.evaluate("MONTH", -1, testDate));
        
        // Test hour, minute, second
        Assert.assertEquals("2023-10-04", dateSub.evaluate("HOUR", 24, testDate));
        Assert.assertEquals("2023-10-04", dateSub.evaluate("MINUTE", 1440, testDate));
        Assert.assertEquals("2023-10-04", dateSub.evaluate("SECOND", 86400, testDate));
        
        // Test null inputs
        Assert.assertNull(dateSub.evaluate(null, 1, testDate));
        Assert.assertNull(dateSub.evaluate("YEAR", null, testDate));
        Assert.assertNull(dateSub.evaluate("YEAR", 1, null));
        
        // Test invalid unit
        Assert.assertNull(dateSub.evaluate("INVALID_UNIT", 1, testDate));
    }

    @Test
    public void testEvaluateWithLocalDateTime() {
        LocalDateTime testDateTime = LocalDateTime.parse("2023-10-05T14:30:45");
        
        // Test positive days
        Assert.assertEquals("2023-10-04 14:30:45", dateSub.evaluate(testDateTime, 1));
        Assert.assertEquals("2023-09-28 14:30:45", dateSub.evaluate(testDateTime, 7));
        
        // Test negative days (which effectively become additions)
        Assert.assertEquals("2023-10-06 14:30:45", dateSub.evaluate(testDateTime, -1));
        Assert.assertEquals("2023-10-12 14:30:45", dateSub.evaluate(testDateTime, -7));
        
        // Test null inputs
        Assert.assertNull(dateSub.evaluate((LocalDateTime)null, 1));
        Assert.assertNull(dateSub.evaluate(testDateTime, null));
    }
}
