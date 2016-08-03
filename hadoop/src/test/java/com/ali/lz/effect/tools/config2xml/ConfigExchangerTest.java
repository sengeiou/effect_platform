/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.easymock.classextension.EasyMock.createStrictControl;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.expect;

import java.util.ArrayList;
import java.util.List;

import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.ConfigExchanger;
import com.ali.lz.effect.tools.config2xml.MySQLManager;
import com.ali.lz.effect.tools.config2xml.PlanConfigResult;
import com.ali.lz.effect.tools.config2xml.PlanShareResult;

/**
 * @author jiuling.ypf
 * 
 */
public class ConfigExchangerTest {

    private IMocksControl control;
    private MySQLManager dbManager;
    private ConfigExchanger exchanger;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        dbManager = control.createMock(MySQLManager.class);
        exchanger = new ConfigExchanger(dbManager);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.ConfigExchanger#exchangeConfig()}
     * .
     */
    @Test
    public void testExchangeConfig() {
        List<PlanShareResult> planShareList = new ArrayList<PlanShareResult>();
        planShareList.add(new PlanShareResult(120, 1));
        planShareList.add(new PlanShareResult(121, 2));
        expect(dbManager.queryPlanShare()).andReturn(planShareList);

        List<PlanConfigResult> planConfigList = new ArrayList<PlanConfigResult>();
        planConfigList.add(new PlanConfigResult(120, 10, "http://www.taobao.com", "102,103", 1, 1, "101,102", 1, 1,
                "1,2,3"));
        planConfigList.add(new PlanConfigResult(121, 20, "http://www.etao.com", "105", 1, 2, "102,103", 1, 1, "1,2,3"));
        expect(dbManager.queryPlanConfig(planShareList)).andReturn(planConfigList);

        List<Integer> pathIdList1 = new ArrayList<Integer>();
        expect(dbManager.queryPathId(120)).andReturn(pathIdList1);

        List<Integer> pathIdList2 = new ArrayList<Integer>();
        pathIdList2.add(new Integer(1));
        pathIdList2.add(new Integer(2));
        expect(dbManager.queryPathId(121)).andReturn(pathIdList2);

        final String[] pathDatas = { "[{name:\"aaa\",url:\"http://www.taobao.com\",step:1}]",
                "[{name:\"bbb\",url:\"http://www.tmall.com\",step:2}]" };

        expect(dbManager.queryPathConfig(1)).andReturn(pathDatas[0]);
        expect(dbManager.queryPathConfig(2)).andReturn(pathDatas[1]);

        expect(dbManager.deletePlanShare(planShareList)).andReturn(true);

        control.replay();

        exchanger.exchangeConfig();

        control.verify();
        control.reset();

        assertTrue(true);
    }

}
