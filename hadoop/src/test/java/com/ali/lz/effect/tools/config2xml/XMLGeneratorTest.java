/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Element;

import com.ali.lz.effect.tools.config2xml.KeyValuePair;
import com.ali.lz.effect.tools.config2xml.XMLGenerator;

/**
 * @author jiuling.ypf
 * 
 */
public class XMLGeneratorTest {

    private XMLGenerator xmlGen;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        File file = new File("report_" + 54321 + ".xml"); // 删除XML文件
        if (file.exists()) {
            boolean flag = file.delete();
            if (!flag) // 删除失败的话，gc后再次删除！
            {
                System.gc();
                Thread.sleep(500);
                file.delete();
            }
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        xmlGen = new XMLGenerator("./", 54321);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        xmlGen = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.XMLGenerator#createElement(org.w3c.dom.Element, java.lang.String)}
     * .
     */
    @Test
    public void testCreateElementElementString() {
        Element root = xmlGen.createElement(null, "root");
        assertEquals(root.getNodeName(), "root");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.XMLGenerator#createElement(org.w3c.dom.Element, java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testCreateElementElementStringListOfKeyValuePair() {
        List<KeyValuePair> attrList = new ArrayList<KeyValuePair>();
        attrList.add(new KeyValuePair("id", "123"));
        attrList.add(new KeyValuePair("name", "test"));
        Element members = xmlGen.createElement(null, "members", attrList);
        assertEquals(members.getNodeName(), "members");
        assertEquals(members.getAttribute("id"), "123");
        assertEquals(members.getAttribute("name"), "test");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.XMLGenerator#createElement(org.w3c.dom.Element, java.lang.String, java.lang.String)}
     * .
     */
    @Test
    public void testCreateElementElementStringString() {
        Element root = xmlGen.createElement(null, "root", "123");
        assertEquals(root.getNodeName(), "root");
        assertEquals(root.getTextContent(), "123");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.XMLGenerator#outputXMLFile()}.
     */
    @Test
    public void testOutputXMLFile() {
        Element root = xmlGen.createElement(null, "root");
        List<KeyValuePair> attrList = new ArrayList<KeyValuePair>();
        attrList.add(new KeyValuePair("id", "123"));
        attrList.add(new KeyValuePair("name", "test"));
        Element members = xmlGen.createElement(root, "members", attrList);
        xmlGen.createElement(members, "member", "123");
        xmlGen.createElement(members, "member", "456");
        xmlGen.createElement(root, "groups", attrList);
        boolean flag = xmlGen.outputXMLFile();
        assertTrue(flag);
    }

}
