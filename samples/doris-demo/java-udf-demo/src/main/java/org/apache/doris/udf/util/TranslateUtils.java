package org.apache.doris.udf.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TranslateUtils {

    /**
     * If a code point needs to be replaced with another code point, this map with store the mapping.
     */
    private static final Map<Integer, Integer> replacementMap = new HashMap<Integer, Integer>();


    /**
     * This set stores all the code points which needed to be deleted from the input string. The
     * objects in deletionSet and keys in replacementMap are mutually exclusive
     */
    private static final Set<Integer> deletionSet = new HashSet<Integer>();

    /**
     * The values of from parameter from the previous evaluate() call.
     */
    private static String lastFrom = null;
    /**
     * The values of to parameter from the previous evaluate() call.
     */
    private static String lastTo = null;

    /**
     * Pre-processes the from and to strings by calling {@link #populateMappings(String, String)} if
     * necessary.
     *
     * @param from
     *          from string to be used for translation
     * @param to
     *          to string to be used for translation
     */
    public static void populateMappingsIfNecessary(String from, String to) {
        // If the from and to strings haven't changed, we don't need to preprocess again to regenerate
        // the mappings of code points that need to replaced or deleted
        if ((lastFrom == null) || (lastTo == null) || !from.equals(lastFrom) || !to.equals(lastTo)) {
            populateMappings(from, to);

            // Need to deep copy here since doing something like lastFrom = from instead, will make
            // lastFrom point to the same Text object which would make from.equals(lastFrom) always true
            lastFrom = from;
            lastTo = to;
        }
    }

    /**
     * Pre-process the from and to strings populate {@link #replacementMap} and {@link #deletionSet}.
     *
     * @param from
     *          from string to be used for translation
     * @param to
     *          to string to be used for translation
     */
    private static void populateMappings(String from, String to) {
        replacementMap.clear();
        deletionSet.clear();

        ByteBuffer fromBytes = ByteBuffer.wrap(from.getBytes(), 0, from.length());
        ByteBuffer toBytes = ByteBuffer.wrap(to.getBytes(), 0, to.length());

        // Traverse through the from string, one code point at a time
        while (fromBytes.hasRemaining()) {
            // This will also move the iterator ahead by one code point
            int fromCodePoint = ByteUtils.bytesToCodePoint(fromBytes);
            // If the to string has more code points, make sure to traverse it too
            if (toBytes.hasRemaining()) {
                int toCodePoint = ByteUtils.bytesToCodePoint(toBytes);
                // If the code point from from string already has a replacement or is to be deleted, we
                // don't need to do anything, just move on to the next code point
                if (replacementMap.containsKey(fromCodePoint) || deletionSet.contains(fromCodePoint)) {
                    continue;
                }
                replacementMap.put(fromCodePoint, toCodePoint);
            } else {
                // If the code point from from string already has a replacement or is to be deleted, we
                // don't need to do anything, just move on to the next code point
                if (replacementMap.containsKey(fromCodePoint) || deletionSet.contains(fromCodePoint)) {
                    continue;
                }
                deletionSet.add(fromCodePoint);
            }
        }
    }

    /**
     * Translates the input string based on {@link #replacementMap} and {@link #deletionSet} and
     * returns the translated string.
     *
     * @param input
     *          input string to perform the translation on
     * @return translated string
     */
    public static String processInput(String input) {
        StringBuilder resultBuilder = new StringBuilder();
        // Obtain the byte buffer from the input string so we can traverse it code point by code point
        ByteBuffer inputBytes = ByteBuffer.wrap(input.getBytes(), 0, input.length());
        // Traverse the byte buffer containing the input string one code point at a time
        while (inputBytes.hasRemaining()) {
            int inputCodePoint = ByteUtils.bytesToCodePoint(inputBytes);
            // If the code point exists in deletion set, no need to emit out anything for this code point.
            // Continue on to the next code point
            if (deletionSet.contains(inputCodePoint)) {
                continue;
            }

            Integer replacementCodePoint = replacementMap.get(inputCodePoint);
            // If a replacement exists for this code point, emit out the replacement and append it to the
            // output string. If no such replacement exists, emit out the original input code point
            char[] charArray = Character.toChars((replacementCodePoint != null) ? replacementCodePoint
                    : inputCodePoint);
            resultBuilder.append(charArray);
        }
        return resultBuilder.toString();
    }
}
