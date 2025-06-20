package org.apache.doris.udf.str;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Comprehensive unit tests for FormatV1 UDF
 * Tests the intelligent type conversion functionality and all supported format specifiers
 */
public class FormatV1Test {

    private FormatV1 formatV1;

    @Before
    public void setUp() {
        formatV1 = new FormatV1();
    }

    // ========== Basic Null and Empty Handling Tests ==========

    @Test
    public void testEvaluateWithNullFormat() {
        assertNull(formatV1.evaluate(null, "123"));
    }

    @Test
    public void testEvaluateWithEmptyFormat() {
        assertNull(formatV1.evaluate("", "123"));
    }

    @Test
    public void testEvaluateWithWhitespaceFormat() {
        assertNull(formatV1.evaluate("   ", "123"));
    }

    @Test
    public void testEvaluateWithNullArg() {
        assertNull(formatV1.evaluate("%.2f", (String) null));
    }

    // ========== Integer Format Conversion Tests ==========

    @Test
    public void testIntegerFormatWithStringArg() {
        assertEquals("123", formatV1.evaluate("%d", "123"));
    }

    @Test
    public void testIntegerFormatWithNegativeString() {
        assertEquals("-456", formatV1.evaluate("%d", "-456"));
    }

    @Test
    public void testIntegerFormatWithLeadingWhitespace() {
        assertEquals("789", formatV1.evaluate("%d", "  789  "));
    }

    @Test
    public void testHexFormatWithDecimalString() {
        assertEquals("ff", formatV1.evaluate("%x", "255"));
    }

    @Test
    public void testHexFormatUppercaseWithDecimalString() {
        assertEquals("FF", formatV1.evaluate("%X", "255"));
    }

    @Test
    public void testOctalFormatWithDecimalString() {
        assertEquals("100", formatV1.evaluate("%o", "64"));
    }

    // ========== Hex and Octal Parsing Tests ==========

    @Test
    public void testHexStringParsing() {
        assertEquals("255", formatV1.evaluate("%d", "0xFF"));
    }

    @Test
    public void testHexStringParsingLowercase() {
        assertEquals("255", formatV1.evaluate("%d", "0xff"));
    }

    @Test
    public void testOctalStringParsing() {
        assertEquals("63", formatV1.evaluate("%d", "077"));
    }

    @Test
    public void testOctalStringParsingLarger() {
        assertEquals("64", formatV1.evaluate("%d", "0100"));
    }

    // ========== Floating Point Format Conversion Tests ==========

    @Test
    public void testFloatFormatWithStringArg() {
        assertEquals("14000.00", formatV1.evaluate("%.2f", "14000"));
    }

    @Test
    public void testFloatFormatWithDecimalString() {
        assertEquals("3.142", formatV1.evaluate("%.3f", "3.14159"));
    }

    @Test
    public void testFloatFormatWithNegativeString() {
        assertEquals("-123.46", formatV1.evaluate("%.2f", "-123.456"));
    }

    @Test
    public void testScientificFormatWithString() {
        assertEquals("1.400000e+04", formatV1.evaluate("%e", "14000"));
    }

    @Test
    public void testScientificFormatUppercaseWithString() {
        assertEquals("1.400000E+04", formatV1.evaluate("%E", "14000"));
    }

    @Test
    public void testGeneralFormatWithString() {
        String result = formatV1.evaluate("%g", "14000");
        assertTrue(result.equals("14000.0") || result.equals("14000"));
    }

    // ========== Character Format Tests ==========

    @Test
    public void testCharacterFormatWithString() {
        assertEquals("A", formatV1.evaluate("%c", "A"));
    }

    @Test
    public void testCharacterFormatWithMultiCharString() {
        assertEquals("H", formatV1.evaluate("%c", "Hello"));
    }

    @Test
    public void testCharacterFormatWithEmptyString() {
        // Empty string with character format should throw an exception
        try {
            formatV1.evaluate("%c", "");
            fail("Expected IllegalArgumentException for empty string with character format");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid format string or parameter mismatch"));
        }
    }

    // ========== Boolean Format Tests ==========

    @Test
    public void testBooleanFormatWithTrueString() {
        assertEquals("true", formatV1.evaluate("%b", "true"));
    }

    @Test
    public void testBooleanFormatWithFalseString() {
        assertEquals("false", formatV1.evaluate("%b", "false"));
    }

    @Test
    public void testBooleanFormatWithRandomString() {
        assertEquals("false", formatV1.evaluate("%b", "random"));
    }

    @Test
    public void testBooleanFormatUppercaseWithTrueString() {
        assertEquals("TRUE", formatV1.evaluate("%B", "true"));
    }

    // ========== String Format Tests ==========

    @Test
    public void testStringFormatWithString() {
        assertEquals("hello", formatV1.evaluate("%s", "hello"));
    }

    @Test
    public void testStringFormatUppercaseWithString() {
        assertEquals("HELLO", formatV1.evaluate("%S", "hello"));
    }

    // ========== Strongly Typed Double Parameter Tests ==========

    @Test
    public void testDoubleParameterWithFloatFormat() {
        assertEquals("14000.00", formatV1.evaluate("%.2f", 14000.0));
    }

    @Test
    public void testDoubleParameterWithNullFormat() {
        assertNull(formatV1.evaluate(null, 14000.0));
    }

    @Test
    public void testDoubleParameterWithNullArg() {
        assertNull(formatV1.evaluate("%.2f", (Double) null));
    }

    // ========== Error Handling Tests ==========

    @Test
    public void testInvalidFormatString() {
        assertThrows(IllegalArgumentException.class, () -> {
            formatV1.evaluate("%s %s", "onlyOneArg");
        });
    }

    @Test
    public void testInvalidConversionFallback() {
        // When string cannot be converted to number, it should remain as string
        // This should cause a format exception since %d expects a number
        assertThrows(IllegalArgumentException.class, () -> {
            formatV1.evaluate("%d", "notANumber");
        });
    }

    // ========== Edge Cases ==========

    @Test
    public void testEmptyStringConversion() {
        // Empty strings should not be converted, remain as string
        assertThrows(IllegalArgumentException.class, () -> {
            formatV1.evaluate("%d", "");
        });
    }

    @Test
    public void testWhitespaceOnlyStringConversion() {
        // Whitespace-only strings should not be converted
        assertThrows(IllegalArgumentException.class, () -> {
            formatV1.evaluate("%d", "   ");
        });
    }

    // ========== Additional Format Conversion Tests ==========

    @Test
    public void testDecimalFormatConversion() {
        assertEquals("123", formatV1.evaluate("%d", "123"));
    }

    @Test
    public void testHexFormatConversion() {
        assertEquals("ff", formatV1.evaluate("%x", "255"));
    }

    @Test
    public void testHexUppercaseFormatConversion() {
        assertEquals("FF", formatV1.evaluate("%X", "255"));
    }

    @Test
    public void testOctalFormatConversion() {
        assertEquals("100", formatV1.evaluate("%o", "64"));
    }

    @Test
    public void testFloatFormatConversion() {
        assertEquals("123.46", formatV1.evaluate("%.2f", "123.456"));
    }

    @Test
    public void testScientificFormatConversion() {
        assertEquals("1.000000e+03", formatV1.evaluate("%e", "1000"));
    }

    @Test
    public void testStringFormatConversion() {
        assertEquals("hello", formatV1.evaluate("%s", "hello"));
    }

    @Test
    public void testCharacterFormatConversion() {
        assertEquals("A", formatV1.evaluate("%c", "A"));
    }

    @Test
    public void testBooleanFormatConversion() {
        assertEquals("true", formatV1.evaluate("%b", "true"));
    }

    // ========== Complex Format String Tests ==========

    @Test
    public void testComplexFormatWithMultipleSpecifiers() {
        // This would require multiple parameters, but FormatV1 only supports single parameter
        // So this test verifies the current limitation
        String result = formatV1.evaluate("Value: %d", "42");
        assertEquals("Value: 42", result);
    }

    // ========== Integration Tests ==========

    @Test
    public void testOriginalFailingCase() {
        // This is the original case that FormatV1 was designed to fix
        String result = formatV1.evaluate("%.2f", "14000");
        assertEquals("14000.00", result);
    }

    @Test
    public void testComparisonWithStronglyTypedVersion() {
        // Both should produce the same result
        String stringResult = formatV1.evaluate("%.2f", "14000");
        String doubleResult = formatV1.evaluate("%.2f", 14000.0);
        assertEquals(stringResult, doubleResult);
    }

    // ========== Edge Case Tests for Null Safety ==========

    @Test
    public void testFormatSpecifierEdgeCases() {
        // Test with malformed format strings that might cause issues
        try {
            // This should not cause null pointer, but might cause other exceptions
            String result = formatV1.evaluate("%", "123");
            // If it doesn't throw an exception, that's also valid behavior
        } catch (IllegalArgumentException e) {
            // Expected behavior for malformed format
            assertTrue(e.getMessage().contains("Invalid format string"));
        }
    }
}
