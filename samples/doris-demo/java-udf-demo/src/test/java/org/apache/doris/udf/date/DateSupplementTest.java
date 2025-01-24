package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DateSupplementTest {
    private DateSupplement dateSupplement;

    @Before
    public void setUp() {
        dateSupplement = new DateSupplement();
    }

    @Test
    public void testEvaluateWithFullDate() {
        // Test with full date format (yyyy-MM-dd)
        Assert.assertEquals("2023-10-05 00:00:00", dateSupplement.evaluate("2023-10-05"));
        Assert.assertEquals("2023-12-31 00:00:00", dateSupplement.evaluate("2023-12-31"));
        Assert.assertEquals("2024-02-29 00:00:00", dateSupplement.evaluate("2024-02-29")); // Leap year
    }

    @Test
    public void testEvaluateWithYearMonth() {
        // Test with year-month format (yyyy-MM)
        Assert.assertEquals("2023-10-01 00:00:00", dateSupplement.evaluate("2023-10"));
        Assert.assertEquals("2023-12-01 00:00:00", dateSupplement.evaluate("2023-12"));
        Assert.assertEquals("2024-02-01 00:00:00", dateSupplement.evaluate("2024-02"));
    }

    @Test
    public void testEvaluateWithYear() {
        // Test with year only format (yyyy)
        Assert.assertEquals("2023-01-01 00:00:00", dateSupplement.evaluate("2023"));
        Assert.assertEquals("2024-01-01 00:00:00", dateSupplement.evaluate("2024"));
    }

    @Test
    public void testEvaluateWithDateTime() {
        // Test with full datetime format (yyyy-MM-dd HH:mm:ss)
        Assert.assertEquals("2023-10-05 14:30:45", dateSupplement.evaluate("2023-10-05 14:30:45"));
        Assert.assertEquals("2023-12-31 23:59:59", dateSupplement.evaluate("2023-12-31 23:59:59"));
    }

    @Test
    public void testEvaluateWithNullAndEmpty() {
        // Test with null and empty inputs
        Assert.assertNull(dateSupplement.evaluate(null));
        Assert.assertEquals("", dateSupplement.evaluate(""));
    }

    @Test
    public void testEvaluateWithInvalidInput() {
        // Test with invalid date formats - should return input as is
        Assert.assertEquals("invalid-date", dateSupplement.evaluate("invalid-date"));
        Assert.assertEquals("2023/10/05", dateSupplement.evaluate("2023/10/05"));
        Assert.assertEquals("20231005", dateSupplement.evaluate("20231005"));
        Assert.assertEquals("2023-13-45", dateSupplement.evaluate("2023-13-45")); // Invalid month and day
    }
}
