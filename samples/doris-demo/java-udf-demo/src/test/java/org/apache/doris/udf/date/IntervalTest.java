package org.apache.doris.udf.date;

import org.junit.Assert;
import org.junit.Test;

public class IntervalTest {

    @Test
    public void testIntervalConstructorAndGetters() {
        Interval interval = new Interval(1, 2, 3, 4, 5, 6);
        
        // Test all getters
        Assert.assertEquals(1, interval.getYears());
        Assert.assertEquals(2, interval.getMonths());
        Assert.assertEquals(3, interval.getDays());
        Assert.assertEquals(4, interval.getHours());
        Assert.assertEquals(5, interval.getMinutes());
        Assert.assertEquals(6, interval.getSeconds());
    }

    @Test
    public void testParseIntervalWithPositiveValues() {
        // Test parsing with all positive values
        Interval interval = Interval.parseInterval("0001-02-03 04:05:06");
        
        Assert.assertEquals(1, interval.getYears());
        Assert.assertEquals(2, interval.getMonths());
        Assert.assertEquals(3, interval.getDays());
        Assert.assertEquals(4, interval.getHours());
        Assert.assertEquals(5, interval.getMinutes());
        Assert.assertEquals(6, interval.getSeconds());
    }

    @Test
    public void testParseIntervalWithZeros() {
        // Test parsing with all zeros
        Interval interval = Interval.parseInterval("0000-00-00 00:00:00");
        
        Assert.assertEquals(0, interval.getYears());
        Assert.assertEquals(0, interval.getMonths());
        Assert.assertEquals(0, interval.getDays());
        Assert.assertEquals(0, interval.getHours());
        Assert.assertEquals(0, interval.getMinutes());
        Assert.assertEquals(0, interval.getSeconds());
    }

    @Test
    public void testParseIntervalWithLargeValues() {
        // Test parsing with large values
        Interval interval = Interval.parseInterval("9999-12-31 23:59:59");
        
        Assert.assertEquals(9999, interval.getYears());
        Assert.assertEquals(12, interval.getMonths());
        Assert.assertEquals(31, interval.getDays());
        Assert.assertEquals(23, interval.getHours());
        Assert.assertEquals(59, interval.getMinutes());
        Assert.assertEquals(59, interval.getSeconds());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testParseIntervalWithInvalidFormat() {
        // Test with invalid format should throw ArrayIndexOutOfBoundsException
        // because the string split will fail first
        Interval.parseInterval("invalid-format");
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testParseIntervalWithMissingTimePart() {
        // Test with missing time part
        Interval.parseInterval("0001-02-03");
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testParseIntervalWithMissingDatePart() {
        // Test with missing date components
        Interval.parseInterval("0001-02");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseIntervalWithNonNumericValues() {
        // Test with properly formatted string but non-numeric values
        Interval.parseInterval("abcd-ef-gh ij:kl:mn");
    }
}
