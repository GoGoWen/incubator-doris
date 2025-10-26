package org.apache.doris.udf.str;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Split extends UDF {

    /**
     * split(String, String)
     * 
     * SELECT split('a:1,b:2,c:3', ',');
     * 
     * @param input
     * @param entryDelimiter
     * @return
     */
    public ArrayList<String> evaluate(String str, String delimiter) {
        if (str == null || delimiter == null) {
            return null;
        }

        ArrayList<String> result = new ArrayList<>();
        int start = 0;
        int end;
        while ((end = str.indexOf(delimiter, start)) != -1) {
            result.add(str.substring(start, end));
            start = end + delimiter.length();
        }
        result.add(str.substring(start));

        return result;
    }

    /**
     * split(String, String, Integer)
     *
     * SELECT split('a:1,b:2,c:3', ',');
     *
     * @param str
     * @param delimiter
     * @param limit
     * @return
     */
    public ArrayList<String> evaluate(String str, String delimiter, Integer limit) {
        // Parameter validation
        if (delimiter == null) {
            throw new IllegalArgumentException("Delimiter cannot be null");
        }
        if (delimiter.isEmpty()) {
            throw new IllegalArgumentException("Delimiter cannot be empty");
        }
        if (limit == null) {
            throw new IllegalArgumentException("Limit parameter cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, current value: " + limit);
        }

        if (str == null) {
            return null;
        }

        if (str.isEmpty()) {
            ArrayList<String> result = new ArrayList<>();
            result.add("");
            return result;
        }

        // When limit is 1, return the original string directly
        if (limit == 1) {
            ArrayList<String> result = new ArrayList<>();
            result.add(str);
            return result;
        }

        // Execute string splitting
        return splitString(str, delimiter, limit);
    }

    /**
     * Core splitting algorithm implementation
     * Optimized based on Presto's split function logic
     *
     * @param str String to be split
     * @param delimiter Delimiter
     * @param limit Split limit
     * @return Split result list
     */
    private ArrayList<String> splitString(String str, String delimiter, int limit) {
        ArrayList<String> result = new ArrayList<>();

        // If the string does not contain the delimiter, return the original string directly
        if (!str.contains(delimiter)) {
            result.add(str);
            return result;
        }

        int delimiterLength = delimiter.length();
        int startIndex = 0;
        int partCount = 0;

        // Loop to find delimiters and split the string
        while (partCount < limit - 1) {
            int delimiterIndex = str.indexOf(delimiter, startIndex);

            // If no more delimiters are found, break the loop
            if (delimiterIndex == -1) {
                break;
            }

            // Add the current part to the result list
            result.add(str.substring(startIndex, delimiterIndex));
            partCount++;

            // Update the starting position for the next search
            startIndex = delimiterIndex + delimiterLength;
        }

        // Add the remaining string as the last part
        result.add(str.substring(startIndex));

        return result;
    }
}
