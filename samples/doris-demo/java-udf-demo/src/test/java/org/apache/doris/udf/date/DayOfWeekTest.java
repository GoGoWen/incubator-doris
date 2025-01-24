package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.joda.time.LocalDateTime;

public class DayOfWeekTest {
    private DayOfWeek dayOfWeek;

    @Before
    public void setUp() {
        dayOfWeek = new DayOfWeek();
    }

    @Test
    public void testEvaluateAllDaysOfWeek() {
        // Test all days of a week (2023-10-01 was a Sunday)
        Assert.assertEquals(7.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 1, 0, 0)), 0.0);  // Sunday
        Assert.assertEquals(1.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 2, 0, 0)), 0.0);  // Monday
        Assert.assertEquals(2.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 3, 0, 0)), 0.0);  // Tuesday
        Assert.assertEquals(3.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 4, 0, 0)), 0.0);  // Wednesday
        Assert.assertEquals(4.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 5, 0, 0)), 0.0);  // Thursday
        Assert.assertEquals(5.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 6, 0, 0)), 0.0);  // Friday
        Assert.assertEquals(6.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 7, 0, 0)), 0.0);  // Saturday
    }

    @Test
    public void testEvaluateWithDifferentTimes() {
        // Test that time of day doesn't affect the day of week
        Assert.assertEquals(4.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 5, 0, 0, 0)), 0.0);    // Start of day
        Assert.assertEquals(4.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 5, 12, 30, 45)), 0.0); // Middle of day
        Assert.assertEquals(4.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 5, 23, 59, 59)), 0.0); // End of day
    }

    @Test
    public void testEvaluateWithEdgeCases() {
        // Test year boundaries
        Assert.assertEquals(7.0, dayOfWeek.evaluate(new LocalDateTime(2023, 12, 31, 0, 0)), 0.0); // Last day of 2023
        Assert.assertEquals(1.0, dayOfWeek.evaluate(new LocalDateTime(2024, 1, 1, 0, 0)), 0.0);   // First day of 2024
        
        // Test month boundaries
        Assert.assertEquals(7.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 1, 0, 0)), 0.0);  // First day of month
        Assert.assertEquals(2.0, dayOfWeek.evaluate(new LocalDateTime(2023, 10, 31, 0, 0)), 0.0); // Last day of month
        
        // Test leap year
        Assert.assertEquals(4.0, dayOfWeek.evaluate(new LocalDateTime(2024, 2, 29, 0, 0)), 0.0);  // Leap day
    }

    @Test
    public void testEvaluateWithNull() {
        // Test null input
        Assert.assertNull(dayOfWeek.evaluate(null));
    }
}
