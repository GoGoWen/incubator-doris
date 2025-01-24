package org.apache.doris.udf.num;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PModTest {

    @Test
    public void testEvaluateBothNull() {
        PMod pMod = new PMod();
        assertNull(pMod.evaluate(null, null));
    }

    @Test
    public void testEvaluateNum1Null() {
        PMod pMod = new PMod();
        assertNull(pMod.evaluate(null, 5.0));
    }

    @Test
    public void testEvaluateNum2Null() {
        PMod pMod = new PMod();
        assertNull(pMod.evaluate(10.0, null));
    }

    @Test
    public void testEvaluateBothPositive() {
        PMod pMod = new PMod();
        assertEquals(1.0, pMod.evaluate(5.0, 2.0), 0.0);
    }

    @Test
    public void testEvaluateNum1Negative() {
        PMod pMod = new PMod();
        assertEquals(1.0, pMod.evaluate(-5.0, 2.0), 0.0);
    }

    @Test
    public void testEvaluateNum2Negative() {
        PMod pMod = new PMod();
        assertEquals(-1.0, pMod.evaluate(5.0, -2.0), 0.0);
    }

    @Test
    public void testEvaluateBothNegative() {
        PMod pMod = new PMod();
        assertEquals(-1.0, pMod.evaluate(-5.0, -2.0), 0.0);
    }

}
