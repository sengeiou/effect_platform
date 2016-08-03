/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.JsonUtil;
import com.ali.lz.effect.tools.config2xml.PathDataRecord;

/**
 * @author jiuling.ypf
 * 
 */
public class JsonUtilTest {

    private List<PathDataRecord> pathList;

    private String pathStr;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        pathList = new ArrayList<PathDataRecord>();
        for (int i = 0; i < 2; i++) {
            PathDataRecord p = new PathDataRecord("name" + i, "http://lz.taobao.com", i);
            pathList.add(p);
        }
        pathStr = "[{\"name\":\"name0\",\"url\":\"http://lz.taobao.com\",\"step\":0},{\"name\":\"name1\",\"url\":\"http://lz.taobao.com\",\"step\":1}]";

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        pathList.clear();
        pathList = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.JsonUtil#toJson(java.util.List)}
     * .
     */
    @Test
    public void testToJson() {
        assertEquals(JsonUtil.toJson(pathList), pathStr);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.JsonUtil#fromJson(java.lang.String)}
     * .
     */
    @Test
    public void testFromJson() {
        List<PathDataRecord> pathList1 = JsonUtil.fromJson(pathStr);
        for (int i = 0; i < pathList.size(); i++) {
            PathDataRecord pathData = pathList.get(i);
            PathDataRecord pathData1 = pathList1.get(i);
            assertEquals(pathData.getName(), pathData1.getName());
            assertEquals(pathData.getUrl(), pathData1.getUrl());
            assertEquals(pathData.getStep(), pathData1.getStep());
        }
    }

}
