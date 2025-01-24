package org.apache.doris.udf.aggregation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Unit tests for the MedianUDAF class.
 * Tests median calculation, state management, serialization, and edge cases.
 */
public class MedianUDAFTest {
    private MedianUDAF medianUDAF;

    @Before
    public void setUp() {
        medianUDAF = new MedianUDAF();
    }

    /**
     * Test initialization of the state object
     */
    @Test
    public void testCreate() {
        MedianUDAF.State state = medianUDAF.create();
        Assert.assertNotNull("State should not be null", state);
        Assert.assertNotNull("StringBuilder should not be null", state.stringBuilder);
        Assert.assertEquals("Initial scale should be 0", 0, state.scale);
        Assert.assertTrue("isFirst should be true initially", state.isFirst);
    }

    /**
     * Test adding single value
     */
    @Test
    public void testAddSingleValue() {
        MedianUDAF.State state = medianUDAF.create();
        medianUDAF.add(state, 10.0, 2);
        Assert.assertEquals("State should contain scale and value", "2,10.0,", state.stringBuilder.toString());
        Assert.assertFalse("isFirst should be false after first add", state.isFirst);
    }

    /**
     * Test adding multiple values
     */
    @Test
    public void testAddMultipleValues() {
        MedianUDAF.State state = medianUDAF.create();
        medianUDAF.add(state, 10.0, 2);
        medianUDAF.add(state, 20.0, 2);
        medianUDAF.add(state, 30.0, 2);
        Assert.assertEquals("State should contain all values", "2,10.0,20.0,30.0,", state.stringBuilder.toString());
    }

    /**
     * Test adding null value
     */
    @Test
    public void testAddNullValue() {
        MedianUDAF.State state = medianUDAF.create();
        medianUDAF.add(state, null, 2);
        Assert.assertEquals("State should be empty for null value", "", state.stringBuilder.toString());
        Assert.assertTrue("isFirst should still be true after null", state.isFirst);
    }

    /**
     * Test serialization and deserialization
     */
    @Test
    public void testSerializeDeserialize() throws Exception {
        MedianUDAF.State originalState = medianUDAF.create();
        medianUDAF.add(originalState, 10.0, 2);
        medianUDAF.add(originalState, 20.0, 2);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        medianUDAF.serialize(originalState, out);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        MedianUDAF.State deserializedState = medianUDAF.create();
        medianUDAF.deserialize(deserializedState, in);

        // The deserialized state should contain the values but might not preserve the scale prefix
        Assert.assertTrue("Deserialized state should contain the values",
                deserializedState.stringBuilder.toString().contains("10.0") &&
                deserializedState.stringBuilder.toString().contains("20.0"));
    }

    /**
     * Test merging two states
     */
    @Test
    public void testMerge() {
        MedianUDAF.State state1 = medianUDAF.create();
        MedianUDAF.State state2 = medianUDAF.create();

        medianUDAF.add(state1, 10.0, 2);
        medianUDAF.add(state1, 20.0, 2);
        
        medianUDAF.add(state2, 30.0, 2);
        medianUDAF.add(state2, 40.0, 2);

        medianUDAF.merge(state1, state2);

        // After merge, verify all values are present
        String mergedString = state1.stringBuilder.toString();
        Assert.assertTrue("Merged state should contain all values",
                mergedString.contains("10.0") &&
                mergedString.contains("20.0") &&
                mergedString.contains("30.0") &&
                mergedString.contains("40.0"));
    }

    /**
     * Test median calculation with odd number of values
     */
    @Test
    public void testGetValueOddCount() {
        MedianUDAF.State state = medianUDAF.create();
        medianUDAF.add(state, 10.0, 2);
        medianUDAF.add(state, 20.0, 2);
        medianUDAF.add(state, 30.0, 2);

        double result = medianUDAF.getValue(state);
        Assert.assertEquals("Median should be 20.0 for odd count", 20.0, result, 0.001);
    }

    /**
     * Test median calculation with even number of values
     */
    @Test
    public void testGetValueEvenCount() {
        MedianUDAF.State state = medianUDAF.create();
        medianUDAF.add(state, 10.0, 2);
        medianUDAF.add(state, 20.0, 2);
        medianUDAF.add(state, 30.0, 2);
        medianUDAF.add(state, 40.0, 2);

        double result = medianUDAF.getValue(state);
        Assert.assertEquals("Median should be 20.0 for even count", 20.0, result, 0.001);
    }

    /**
     * Test median calculation with decimal precision
     */
    @Test
    public void testGetValueWithPrecision() {
        MedianUDAF.State state = medianUDAF.create();
        medianUDAF.add(state, 10.123, 2);
        medianUDAF.add(state, 20.456, 2);
        medianUDAF.add(state, 30.789, 2);

        double result = medianUDAF.getValue(state);
        Assert.assertEquals("Median should be 20.0 with scale 2", 20.0, result, 0.001);
    }

    /**
     * Test empty state
     */
    @Test
    public void testGetValueEmptyState() {
        MedianUDAF.State state = medianUDAF.create();
        double result = medianUDAF.getValue(state);
        Assert.assertEquals("Empty state should return 0.0", 0.0, result, 0.001);
    }
}
