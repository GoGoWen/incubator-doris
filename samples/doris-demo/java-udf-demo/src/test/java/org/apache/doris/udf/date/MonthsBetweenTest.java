package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonthsBetweenTest {
    private MonthsBetween monthsBetween;
    private static final double DELTA = 0.00000001; // Delta for double comparison

    @Before
    public void setUp() {
        monthsBetween = new MonthsBetween();
    }

    @Test
    public void testSameDayOfMonth() {
        // Test dates with same day of month
        Assert.assertEquals(1.0, monthsBetween.evaluate("2023-11-15", "2023-10-15"), DELTA);
        Assert.assertEquals(12.0, monthsBetween.evaluate("2024-10-15", "2023-10-15"), DELTA);
        Assert.assertEquals(-1.0, monthsBetween.evaluate("2023-10-15", "2023-11-15"), DELTA);
    }

    @Test
    public void testEndOfMonth() {
        // Test dates at end of months
        Assert.assertEquals(1.0, monthsBetween.evaluate("2023-03-31", "2023-02-28"), DELTA);
        Assert.assertEquals(1.0, monthsBetween.evaluate("2024-02-29", "2024-01-31"), DELTA);
        Assert.assertEquals(1.0, monthsBetween.evaluate("2023-05-31", "2023-04-30"), DELTA);
    }

    @Test
    public void testWithTime() {
        // Test dates with time components and different days
        Assert.assertEquals(3.94959677, monthsBetween.evaluate("1997-02-28 10:30:00", "1996-10-30"), DELTA);
        Assert.assertEquals(-3.94959677, monthsBetween.evaluate("1996-10-30", "1997-02-28 10:30:00"), DELTA);
        
        // Test same dates with different times but same day - should return 0.0 as per implementation
        Assert.assertEquals(0.0, monthsBetween.evaluate("2023-10-15 12:00:00", "2023-10-15 00:00:00"), DELTA);
        
        // Test dates with different days to show time impact
        Assert.assertEquals(0.03225806, monthsBetween.evaluate("2023-10-16 00:00:00", "2023-10-15 00:00:00"), DELTA);
    }

    @Test
    public void testDifferentDaysWithinMonth() {
        // Test dates with different days within month
        Assert.assertEquals(1.03225806, monthsBetween.evaluate("2023-11-15", "2023-10-14"), DELTA);
        Assert.assertEquals(-1.03225806, monthsBetween.evaluate("2023-10-14", "2023-11-15"), DELTA);
    }

    @Test
    public void testNullAndEmptyInputs() {
        // Test null and empty inputs
        Assert.assertNull(monthsBetween.evaluate(null, "2023-10-15"));
        Assert.assertNull(monthsBetween.evaluate("2023-10-15", null));
        Assert.assertNull(monthsBetween.evaluate(null, null));
        Assert.assertNull(monthsBetween.evaluate("", "2023-10-15"));
        Assert.assertNull(monthsBetween.evaluate("2023-10-15", ""));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidDateFormat() {
        // Test invalid date format
        monthsBetween.evaluate("invalid-date", "2023-10-15");
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidDateValues() {
        // Test invalid date values
        monthsBetween.evaluate("2023-13-45", "2023-10-15");
    }

    @Test
    public void testLeapYearCases() {
        // Test leap year scenarios
        Assert.assertEquals(12.0, monthsBetween.evaluate("2024-02-29", "2023-02-28"), DELTA);
        Assert.assertEquals(1.0, monthsBetween.evaluate("2024-02-29", "2024-01-29"), DELTA);
        Assert.assertEquals(1.0, monthsBetween.evaluate("2024-03-31", "2024-02-29"), DELTA);
    }
}
