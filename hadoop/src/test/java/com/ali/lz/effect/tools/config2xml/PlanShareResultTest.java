/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.PlanShareResult;

/**
 * @author jiuling.ypf
 * 
 */
public class PlanShareResultTest {

    private PlanShareResult result;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        result = new PlanShareResult(123, 1);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        result = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanShareResult#getPlanId()}.
     */
    @Test
    public void testGetPlanId() {
        assertEquals(result.getPlanId(), 123);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanShareResult#setPlanId(int)}
     * .
     */
    @Test
    public void testSetPlanId() {
        result.setPlanId(124);
        assertEquals(result.getPlanId(), 124);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanShareResult#getType()}.
     */
    @Test
    public void testGetType() {
        assertEquals(result.getType(), 1);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanShareResult#setType(int)}.
     */
    @Test
    public void testSetType() {
        result.setType(2);
        assertEquals(result.getType(), 2);
    }

}
