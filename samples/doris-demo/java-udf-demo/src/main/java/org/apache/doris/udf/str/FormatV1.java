package org.apache.doris.udf.str;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.IllegalFormatException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Enhanced Format UDF with intelligent type conversion for string parameters
 * This UDF provides printf-style string formatting with automatic type conversion
 * for string parameters when they can be safely converted to numeric types.
 */
public class FormatV1 extends UDF {

    /**
     * Check if string is empty or null (replacement for StringUtils.isEmpty)
     */
    private static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private static final Pattern FORMAT_PATTERN = Pattern.compile(
        "%(?:(\\d+)\\$)?[-#+ 0,(]*(?:\\d+)?(?:\\.\\d+)?[bBhHsScCdoxXeEfFgGaAtT%n]"
    );

    /**
     * Format with single string argument - Main method for Doris UDF
     * Supports intelligent type conversion based on format specifiers
     */
    public String evaluate(String format, String arg) {
        if (isEmpty(format) || arg == null) {
            return null;
        }
        return formatWithConversion(format, arg);
    }

    public String evaluate(String format, Double arg) {
        return evaluateTyped(format, arg);
    }

    /**
     * Handle strongly typed single parameter
     */
    private String evaluateTyped(String format, Object arg) {
        if (isEmpty(format) || arg == null) {
            return null;
        }

        try {
            return String.format(format, arg);
        } catch (IllegalFormatException e) {
            throw new IllegalArgumentException("Invalid format string or parameter mismatch: " + e.getMessage());
        }
    }

    /**
     * Core formatting method with intelligent type conversion
     */
    private String formatWithConversion(String formatStr, Object... values) {
        if (isEmpty(formatStr)) {
            return null;
        }

        // Check for null arguments
        for (Object value : values) {
            if (value == null) {
                return null;
            }
        }

        try {
            Object[] processedValues = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                processedValues[i] = convertValueIfNeeded(formatStr, i, values[i]);
            }
            return String.format(formatStr, processedValues);
        } catch (IllegalFormatException e) {
            throw new IllegalArgumentException("Invalid format string or parameter mismatch: " + e.getMessage());
        } catch (Exception e) {
            throw new IllegalArgumentException("Error processing format parameters: " + e.getMessage());
        }
    }

    /**
     * Convert string values to appropriate types based on format specifiers
     */
    private Object convertValueIfNeeded(String formatStr, int argIndex, Object value) {
        // Only convert string values
        if (!(value instanceof String)) {
            return value;
        }

        String strValue = ((String) value).trim();
        if (strValue.isEmpty()) {
            return value; // Don't convert empty strings
        }

        String formatSpec = extractFormatSpecifier(formatStr, argIndex);
        if (formatSpec == null) {
            return value;
        }

        return convertBasedOnFormat(formatSpec, strValue);
    }

    /**
     * Convert string value based on format specifier
     */
    private Object convertBasedOnFormat(String formatSpec, String strValue) {
        char conversionChar = formatSpec.charAt(formatSpec.length() - 1);

        try {
            switch (conversionChar) {
                case 'd':
                case 'o':
                case 'x':
                case 'X':
                    // Integer formats
                    return parseInteger(strValue);

                case 'f':
                case 'F':
                case 'e':
                case 'E':
                case 'g':
                case 'G':
                case 'a':
                case 'A':
                    // Floating point formats
                    return Double.parseDouble(strValue);

                case 'c':
                    // Character format - take first character
                    return strValue.length() > 0 ? strValue.charAt(0) : strValue;

                case 'b':
                case 'B':
                    // Boolean format
                    return Boolean.parseBoolean(strValue);

                case 's':
                case 'S':
                default:
                    // String format or unknown - keep as string
                    return strValue;
            }
        } catch (NumberFormatException e) {
            // If conversion fails, return original string
            return strValue;
        }
    }

    /**
     * Parse integer with support for different bases
     */
    private Long parseInteger(String strValue) {
        // Handle hex prefix
        if (strValue.toLowerCase().startsWith("0x")) {
            return Long.parseLong(strValue.substring(2), 16);
        }
        // Handle octal prefix
        if (strValue.startsWith("0") && strValue.length() > 1 && strValue.matches("0[0-7]+")) {
            return Long.parseLong(strValue, 8);
        }
        // Default decimal
        return Long.parseLong(strValue);
    }

    /**
     * Extract format specifier for given argument index
     */
    private String extractFormatSpecifier(String formatStr, int argIndex) {
        Matcher matcher = FORMAT_PATTERN.matcher(formatStr);
        int currentIndex = 0;

        while (matcher.find()) {
            String indexGroup = matcher.group(1);
            int specifierIndex = (indexGroup != null) ?
                Integer.parseInt(indexGroup) - 1 : currentIndex++;

            if (specifierIndex == argIndex) {
                return matcher.group();
            }
        }

        return null;
    }
}
