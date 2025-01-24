package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NextDayTest {
    private NextDay nextDay;

    @Before
    public void setUp() {
        nextDay = new NextDay();
    }

    @Test
    public void testNextDayNormalCases() {
        // Test all days of the week
        Assert.assertEquals("2015-01-20", nextDay.evaluate("2015-01-14", "TU")); // Wednesday to Tuesday
        Assert.assertEquals("2015-01-16", nextDay.evaluate("2015-01-14", "FR")); // Wednesday to Friday
        Assert.assertEquals("2015-01-19", nextDay.evaluate("2015-01-14", "MO")); // Wednesday to Monday
        Assert.assertEquals("2015-01-15", nextDay.evaluate("2015-01-14", "TH")); // Wednesday to Thursday
        Assert.assertEquals("2015-01-21", nextDay.evaluate("2015-01-14", "WE")); // Wednesday to next Wednesday
        Assert.assertEquals("2015-01-17", nextDay.evaluate("2015-01-14", "SA")); // Wednesday to Saturday
        Assert.assertEquals("2015-01-18", nextDay.evaluate("2015-01-14", "SU")); // Wednesday to Sunday
    }

    @Test
    public void testNextDayNullInputs() {
        // Test null and empty inputs
        Assert.assertNull(nextDay.evaluate(null, "MO"));
        Assert.assertNull(nextDay.evaluate("2015-01-14", null));
        Assert.assertNull(nextDay.evaluate(null, null));
        Assert.assertNull(nextDay.evaluate("", "MO"));
        Assert.assertNull(nextDay.evaluate("2015-01-14", ""));
    }

    @Test
    public void testNextDayInvalidInputs() {
        // Test invalid date format
        Assert.assertNull(nextDay.evaluate("invalid-date", "MO"));
        // Test invalid day of week
        Assert.assertNull(nextDay.evaluate("2015-01-14", "INVALID"));
        Assert.assertNull(nextDay.evaluate("2015-01-14", "XXX"));
    }

    @Test
    public void testNextDayEdgeCases() {
        // Test month boundary
        Assert.assertEquals("2015-02-02", nextDay.evaluate("2015-01-30", "MO"));
        // Test year boundary
        Assert.assertEquals("2016-01-01", nextDay.evaluate("2015-12-30", "FR"));
        // Test leap year
        Assert.assertEquals("2016-02-29", nextDay.evaluate("2016-02-24", "MO"));
    }
}
