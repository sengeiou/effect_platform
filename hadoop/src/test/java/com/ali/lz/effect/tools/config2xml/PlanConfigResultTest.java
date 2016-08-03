/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.PlanConfigResult;

/**
 * @author jiuling.ypf
 * 
 */
public class PlanConfigResultTest {

    private PlanConfigResult result;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        result = new PlanConfigResult(123, 10, "http://www.taobao.com", "101,102", 0, 1, "101,102", 1, 1, "1,2,3");
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
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getPlanId()}.
     */
    @Test
    public void testGetPlanId() {
        assertEquals(result.getPlanId(), 123);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setPlanId(int)}
     * .
     */
    @Test
    public void testSetPlanId() {
        result.setPlanId(124);
        assertEquals(result.getPlanId(), 124);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getUserId()}.
     */
    @Test
    public void testGetUserId() {
        assertEquals(result.getUserId(), 10);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setUserId(int)}
     * .
     */
    @Test
    public void testSetUserId() {
        result.setUserId(20);
        assertEquals(result.getUserId(), 20);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getEffectUrl()}
     * .
     */
    @Test
    public void testGetEffectUrl() {
        assertEquals(result.getEffectUrl(), "http://www.taobao.com");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setEffectUrl(java.lang.String)}
     * .
     */
    @Test
    public void testSetEffectUrl() {
        result.setEffectUrl("http://www.tmall.com");
        assertEquals(result.getEffectUrl(), "http://www.tmall.com");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getSrcType()}.
     */
    @Test
    public void testGetSrcType() {
        assertEquals(result.getSrcType(), "101,102");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setSrcType(java.lang.String)}
     * .
     */
    @Test
    public void testSetSrcType() {
        result.setSrcType("102,103");
        assertEquals(result.getSrcType(), "102,103");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getPathType()}
     * .
     */
    @Test
    public void testGetPathType() {
        assertEquals(result.getPathType(), 0);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setPathType(int)}
     * .
     */
    @Test
    public void testSetPathType() {
        result.setPathType(1);
        assertEquals(result.getPathType(), 1);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getBelongId()}
     * .
     */
    @Test
    public void testGetBelongId() {
        assertEquals(result.getBelongId(), 1);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setBelongId(int)}
     * .
     */
    @Test
    public void testSetBelongId() {
        result.setBelongId(2);
        assertEquals(result.getBelongId(), 2);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getIndIds()}.
     */
    @Test
    public void testGetIndIds() {
        assertEquals(result.getIndIds(), "101,102");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setIndIds(java.lang.String)}
     * .
     */
    @Test
    public void testSetIndIds() {
        result.setIndIds("102,103");
        assertEquals(result.getIndIds(), "102,103");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getPeriod()}.
     */
    @Test
    public void testGetPeriod() {
        assertEquals(result.getPeriod(), 1);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setPeriod(int)}
     * .
     */
    @Test
    public void testSetPeriod() {
        result.setPeriod(2);
        assertEquals(result.getPeriod(), 2);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getExpireType()}
     * .
     */
    @Test
    public void testGetExpireType() {
        assertEquals(result.getExpireType(), 1);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setExpireType(int)}
     * .
     */
    @Test
    public void testSetExpireType() {
        result.setExpireType(2);
        assertEquals(result.getExpireType(), 2);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#getLinkType()}
     * .
     */
    @Test
    public void testGetLinkType() {
        assertEquals(result.getLinkType(), "1,2,3");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PlanConfigResult#setLinkType(String)}
     * .
     */
    @Test
    public void testSetLinkType() {
        result.setLinkType("2,3,4");
        assertEquals(result.getLinkType(), "2,3,4");
    }

}
