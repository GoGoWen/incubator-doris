package org.apache.doris.udf.aggregation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Unit tests for the AvgDouble UDF (User Defined Function).
 * This test suite verifies the functionality of calculating average values
 * including state management, value aggregation, and serialization.
 */
public class AvgDoubleTest {
    private AvgDouble avgDouble;

    /**
     * Set up method that runs before each test.
     * Creates a new instance of AvgDouble for each test to ensure test isolation.
     */
    @Before
    public void setUp() {
        avgDouble = new AvgDouble();
    }

    /**
     * Tests the create() method of AvgDouble.
     * Verifies that a newly created state has:
     * - sum initialized to 0.0
     * - count initialized to 0
     */
    @Test
    public void testCreate() {
        AvgDouble.State state = avgDouble.create();
        Assert.assertEquals(0.0, state.sum, 0.001);
        Assert.assertEquals(0, state.count.intValue());
    }

    /**
     * Tests the reset() method of AvgDouble.
     * Verifies that after setting some values and calling reset:
     * - sum is reset to 0.0
     * - count is reset to 0
     * This is particularly important for window function support.
     */
    @Test
    public void testReset() {
        AvgDouble.State state = avgDouble.create();
        state.sum = 10.0;
        state.count = 5;
        
        avgDouble.reset(state);
        Assert.assertEquals(0.0, state.sum, 0.001);
        Assert.assertEquals(0, state.count.intValue());
    }

    /**
     * Tests the add() method of AvgDouble.
     * Verifies:
     * - Adding multiple positive values
     * - Handling of null values (should be ignored)
     * - Correct calculation of sum and count
     */
    @Test
    public void testAdd() throws Exception {
        AvgDouble.State state = avgDouble.create();
        
        avgDouble.add(state, 5.0);
        avgDouble.add(state, 15.0);
        avgDouble.add(state, null); // Testing null value
        
        Assert.assertEquals(20.0, state.sum, 0.001);
        Assert.assertEquals(2, state.count.intValue());
    }

    /**
     * Tests the merge() method of AvgDouble.
     * Verifies the ability to merge two states by:
     * - Creating two separate states
     * - Adding different values to each state
     * - Merging the states
     * - Verifying the combined sum and count
     */
    @Test
    public void testMerge() throws Exception {
        AvgDouble.State state1 = avgDouble.create();
        AvgDouble.State state2 = avgDouble.create();
        
        avgDouble.add(state1, 10.0);
        avgDouble.add(state1, 20.0);
        
        avgDouble.add(state2, 30.0);
        avgDouble.add(state2, 40.0);
        
        avgDouble.merge(state1, state2);
        
        Assert.assertEquals(100.0, state1.sum, 0.001);
        Assert.assertEquals(4, state1.count.intValue());
    }

    /**
     * Tests the getValue() method of AvgDouble.
     * Verifies:
     * - Correct average calculation with multiple values
     * - Precision of the calculation
     * Expected average = (10 + 20 + 30) / 3 = 20.0
     */
    @Test
    public void testGetValue() throws Exception {
        AvgDouble.State state = avgDouble.create();
        
        avgDouble.add(state, 10.0);
        avgDouble.add(state, 20.0);
        avgDouble.add(state, 30.0);
        
        double result = avgDouble.getValue(state);
        Assert.assertEquals(20.0, result, 0.001);
    }

    /**
     * Tests the getValue() method with an empty state.
     * Verifies:
     * - Handling of empty state (no values added)
     * - Returns 0.0 as expected when no values are present
     */
    @Test
    public void testGetValueWithEmptyState() throws Exception {
        AvgDouble.State state = avgDouble.create();
        double result = avgDouble.getValue(state);
        Assert.assertEquals(0.0, result, 0.001);
    }

    /**
     * Tests the serialize() and deserialize() methods of AvgDouble.
     * Verifies:
     * - Ability to serialize state to bytes
     * - Ability to deserialize bytes back to state
     * - Correctness of deserialized values
     * This is crucial for distributed computing scenarios where state needs to be transferred between nodes.
     */
    @Test
    public void testSerializeDeserialize() throws Exception {
        AvgDouble.State originalState = avgDouble.create();
        avgDouble.add(originalState, 10.0);
        avgDouble.add(originalState, 20.0);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        avgDouble.serialize(originalState, out);
        
        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        AvgDouble.State deserializedState = avgDouble.create();
        avgDouble.deserialize(deserializedState, in);
        
        // Verify deserialized state
        Assert.assertEquals(deserializedState.sum, originalState.sum, 0.001);
        Assert.assertEquals(deserializedState.count, originalState.count);
    }
}
