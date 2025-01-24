package org.apache.doris.udf.str;

import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class FormatTest {

    private final Format format = new Format();

    @Test
    public void testEvaluateWithNullFormat() {
        assertNull(format.evaluate(null, 10.0f));
    }

    @Test
    public void testEvaluateWithNullFormat1() {
        Format format = new Format();
        assertNull(format.evaluate(null, 10));
    }

    @Test
    public void testEvaluateWithValidFormatAndArg1() {
        Format format = new Format();
        assertEquals("10", format.evaluate("%d", 10));
    }

    @Test
    public void testEvaluateWithNullFormat2() {
        assertNull(format.evaluate(null, "arg", "arg1"));
    }

    @Test
    public void testEvaluateWithNullArg2() {
        assertNull(format.evaluate("format", null, "arg1"));
    }

    @Test
    public void testEvaluateWithNullArg11() {
        assertNull(format.evaluate("format", "arg", null));
    }

    @Test
    public void testEvaluateWithValidFormat() {
        assertEquals("arg arg1", format.evaluate("%s %s", "arg", "arg1"));
    }

    @Test
    public void testEvaluateWithNullFormat3() {
        assertNull(format.evaluate(null, 10.0));
    }

    @Test
    public void testEvaluateWithValidFormatAndArg2() {
        assertEquals("10.0", format.evaluate("%.1f", 10.0));
    }

    @Test
    public void testEvaluateWithInvalidFormat3() {
        try {
            format.evaluate("%s %s", 10.0);
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid format string or parameter mismatch.", e.getMessage());
        }
    }

    @Test
    public void testEvaluateWithNullFormat4() {
        Format format = new Format();
        assertNull(format.evaluate(null, new Date()));
    }

    @Test
    public void testEvaluateWithEmptyFormat() {
        Format format = new Format();
        assertNull(format.evaluate("", new Date()));
    }

    @Test
    public void testEvaluateWithValidFormat1() {
        Format format = new Format();
        Date date = new Date();
        assertEquals(String.format("Date: %tF", date), format.evaluate("Date: %tF", date));
    }

    @Test
    public void testEvaluateWithInvalidFormat4() {
        Format format = new Format();
        Date date = new Date();
        try {
            format.evaluate("%s %s", date);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid format string or parameter mismatch.", e.getMessage());
        }
    }

    @Test
    public void testEvaluateWithNullFormatReturnsNull() {
        assertNull(format.evaluate(null, "arg"));
    }

    @Test
    public void testEvaluateWithEmptyFormatReturnsNull() {
        assertNull(format.evaluate("", "arg"));
    }


    @Test
    public void testEvaluateWithValidFormatAndArgReturnsFormattedString() {
        assertEquals("Hello World", format.evaluate("%s %s", "Hello", "World"));
    }

    @Test
    public void testEvaluateWithInvalidFormatThrowsIllegalArgumentException() {
        try {
            format.evaluate("%s %s %s", "Hello", "World");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid format string or parameter mismatch.", e.getMessage());
        }
    }

    @Test
    public void testEvaluateWithNullFormat5() {
        assertNull(format.evaluate(null, "arg", "arg1", "arg2"));
    }

    @Test
    public void testEvaluateWithNullArg5() {
        assertNull(format.evaluate("format", null, "arg1", "arg2"));
    }

    @Test
    public void testEvaluateWithNullArg12() {
        assertNull(format.evaluate("format", "arg", null, "arg2"));
    }

    @Test
    public void testEvaluateWithNullArg21() {
        assertNull(format.evaluate("format", "arg", "arg1", null));
    }

    @Test
    public void testEvaluateWithAllNonNullArgs() {
        assertEquals("format", format.evaluate("format", "arg", "arg1", "arg2"));
    }

    @Test
    public void testEvaluateWithEmptyFormat1() {
        assertNull(format.evaluate("", "arg", "arg1", "arg2"));
    }

    @Test
    public void testEvaluateWithNullFormatShouldReturnNull() {
        assertNull(format.evaluate(null, "arg", "arg1", "arg2", "arg3", "arg4", "arg5"));
    }

    @Test
    public void testEvaluateWithNullArgsShouldReturnNull() {
        assertNull(format.evaluate("format", null, "arg1", "arg2", "arg3", "arg4", "arg5"));
    }

    @Test
    public void testEvaluateWithValidArgsShouldReturnFormattedString() {
        assertEquals("Hello, World!", format.evaluate("Hello, %s!", "World", "arg1", "arg2", "arg3", "arg4", "arg5"));
    }

    @Test
    public void testFormatWithValidArguments() {
        Format format = new Format();
        String result = format.format("Hello, %s! Today is %s.", "Alice", "Monday");
        assertEquals("Hello, Alice! Today is Monday.", result);
    }

    @Test
    public void testFormatWithNoArguments() {
        Format format = new Format();
        String result = format.format("No arguments");
        assertEquals("No arguments", result);
    }

    @Test
    public void testFormatWithIntegerArgument() {
        Format format = new Format();
        String result = format.format("The answer is %d", 42);
        assertEquals("The answer is 42", result);
    }

    @Test
    public void testFormatWithInvalidFormatString() {
        Format format = new Format();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            format.format("Hello, %s! Today is %s.", "Alice");
        });
        assertEquals("Invalid format string or parameter mismatch.", exception.getMessage());
    }

    @Test
    public void testFormatWithInvalidArgumentType() {
        Format format = new Format();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            format.format("The answer is %d", "forty-two");
        });
        assertEquals("Invalid format string or parameter mismatch.", exception.getMessage());
    }

    @Test
    public void testEvaluateWithNullFormatShouldReturnNull1() {
        assertNull(format.evaluate(null, "arg", "arg1", "arg2", "arg3", "arg4"));
    }

    @Test
    public void testEvaluateWithNullArgShouldReturnNull() {
        assertNull(format.evaluate("format", null, "arg1", "arg2", "arg3", "arg4"));
    }

    @Test
    public void testEvaluateWithNullArg1ShouldReturnNull() {
        assertNull(format.evaluate("format", "arg", null, "arg2", "arg3", "arg4"));
    }

    @Test
    public void testEvaluateWithNullArg2ShouldReturnNull() {
        assertNull(format.evaluate("format", "arg", "arg1", null, "arg3", "arg4"));
    }

    @Test
    public void testEvaluateWithNullArg3ShouldReturnNull() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", null, "arg4"));
    }

    @Test
    public void testEvaluateWithNullArg4ShouldReturnNull() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", "arg3", null));
    }

    @Test
    public void testEvaluateWithValidArgumentsShouldReturnFormattedString() {
        String formatStr = "Hello %s, your age is %s, you have %s children, your balance is %s, and your height is %s";
        String arg = "John";
        String arg1 = "30";
        String arg2 = "2";
        String arg3 = "$1000";
        String arg4 = "180cm";
        String expected = "Hello John, your age is 30, you have 2 children, your balance is $1000, and your height is 180cm";
        assertEquals(expected, format.evaluate(formatStr, arg, arg1, arg2, arg3, arg4));
    }

    @Test
    public void testEvaluateWithNullFormatReturnsNull1() {
        assertNull(format.evaluate(null, "arg", "arg1", "arg2", "arg3"));
    }

    @Test
    public void testEvaluateWithNullArgReturnsNull1() {
        assertNull(format.evaluate("format", null, "arg1", "arg2", "arg3"));
    }

    @Test
    public void testEvaluateWithNullArg1ReturnsNull() {
        assertNull(format.evaluate("format", "arg", null, "arg2", "arg3"));
    }

    @Test
    public void testEvaluateWithNullArg2ReturnsNull() {
        assertNull(format.evaluate("format", "arg", "arg1", null, "arg3"));
    }

    @Test
    public void testEvaluateWithNullArg3ReturnsNull() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", null));
    }

    @Test
    public void testEvaluateWithAllArgsNonNullReturnsFormattedString() {
        String formatStr = "Hello %s, your age is %s, you have %s children, and your salary is %s";
        String arg = "John";
        String arg1 = "30";
        String arg2 = "2";
        String arg3 = "$5000";
        String expected = "Hello John, your age is 30, you have 2 children, and your salary is $5000";
        assertEquals(expected, format.evaluate(formatStr, arg, arg1, arg2, arg3));
    }

    @Test
    public void testEvaluateWithInvalidFormatStringThrowsIllegalArgumentException() {
        String formatStr = "Hello %s, your age is %s, you have %s children, and your salary is %s";
        String arg = "John";
        String arg1 = "30";
        String arg2 = "2";
        String arg3 = "$5000";
        String invalidFormatStr = "Hello %s, your age is %s, you have %s children"; // Missing one argument
        try {
            format.evaluate(invalidFormatStr, arg, arg1, arg2, arg3);
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid format string or parameter mismatch.", e.getMessage());
        }
    }

    @Test
    public void testEvaluateAllArgsNonNull() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7");
        assertEquals("arg arg1 arg2 arg3 arg4 arg5 arg6 arg7", result);
    }

    @Test
    public void testEvaluateFormatNull() {
        assertNull(format.evaluate(null, "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7"));
    }

    @Test
    public void testEvaluateArgNull() {
        assertNull(format.evaluate("%s %s %s %s %s %s %s %s", null, "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7"));
    }

    @Test
    public void testEvaluateWithNullFormatShouldReturnNull2() {
        assertNull(format.evaluate(null, "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6"));
    }

    @Test
    public void testEvaluateWithNullArgShouldReturnNull1() {
        assertNull(format.evaluate("format", null, "arg1", "arg2", "arg3", "arg4", "arg5", "arg6"));
    }

    @Test
    public void testEvaluateWithNullArg1ShouldReturnNull1() {
        assertNull(format.evaluate("format", "arg", null, "arg2", "arg3", "arg4", "arg5", "arg6"));
    }

    @Test
    public void testEvaluateWithNullArg2ShouldReturnNull1() {
        assertNull(format.evaluate("format", "arg", "arg1", null, "arg3", "arg4", "arg5", "arg6"));
    }

    @Test
    public void testEvaluateWithNullArg3ShouldReturnNull1() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", null, "arg4", "arg5", "arg6"));
    }

    @Test
    public void testEvaluateWithNullArg4ShouldReturnNull1() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", "arg3", null, "arg5", "arg6"));
    }

    @Test
    public void testEvaluateWithNullArg5ShouldReturnNull() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", "arg3", "arg4", null, "arg6"));
    }

    @Test
    public void testEvaluateWithNullArg6ShouldReturnNull() {
        assertNull(format.evaluate("format", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", null));
    }

    @Test
    public void testEvaluateWithAllArgsNonNullShouldReturnFormattedString() {
        String formatStr = "Hello %s, arg1: %s, arg2: %s, arg3: %s, arg4: %s, arg5: %s, arg6: %s";
        String expected = "Hello world, arg1: value1, arg2: value2, arg3: value3, arg4: value4, arg5: value5, arg6: value6";
        assertEquals(expected, format.evaluate(formatStr, "world", "value1", "value2", "value3", "value4", "value5", "value6"));
    }

    @Test
    public void testEvaluateWithInvalidFormatStringShouldThrowIllegalArgumentException1() {
        String formatStr = "Hello %s, arg1: %s, arg2: %s, arg3: %s, arg4: %s, arg5: %s, arg6: %s";
        try {
            format.evaluate(formatStr, "world", "value1", "value2", "value3", "value4", "value5");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid format string or parameter mismatch.", e.getMessage());
        }
    }

    @Test
    public void testEvaluateAllArgsNonNull1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8");
        assertEquals("arg arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8", result);
    }

    @Test
    public void testEvaluateFormatNull1() {
        String result = format.evaluate(null, "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArgNull1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", null, "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg1Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", null, "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg2Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", null, "arg3", "arg4", "arg5", "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg3Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", null, "arg4", "arg5", "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg4Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", null, "arg5", "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg5Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", null, "arg6", "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg6Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", null, "arg7", "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg7Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", null, "arg8");
        assertNull(result);
    }

    @Test
    public void testEvaluateAllArgsNonNull2() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", "arg9");
        assertEquals("arg arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9", result);
    }

    @Test
    public void testEvaluateFormatNull2() {
        String result = format.evaluate(null, "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArgNull2() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", null, "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg1Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", null, "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg2Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", null, "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg3Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", null, "arg4", "arg5", "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg4Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", null, "arg5", "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg5Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", null, "arg6", "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg6Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", null, "arg7", "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg7Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", null, "arg8", "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg8Null1() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", null, "arg9");
        assertNull(result);
    }

    @Test
    public void testEvaluateArg9Null() {
        String result = format.evaluate("%s %s %s %s %s %s %s %s %s %s", "arg", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", null);
        assertNull(result);
    }

}
