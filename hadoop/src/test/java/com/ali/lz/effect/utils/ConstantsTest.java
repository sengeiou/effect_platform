package com.ali.lz.effect.utils;

import org.junit.Assert;
import org.junit.Test;

public class ConstantsTest {

    @Test
    public void logTypeTest() {
        Assert.assertEquals(0, Constants.ELogType.ACCESS_LOG.getLogType());
        Assert.assertEquals(1, Constants.ELogType.GMV_LOG.getLogType());
        Assert.assertEquals(2, Constants.ELogType.COLLECT_LOG.getLogType());
        Assert.assertEquals(3, Constants.ELogType.CART_LOG.getLogType());
    }

    @Test
    public void logColumnNumTest() {
        Assert.assertEquals(11, Constants.ELogColumnNum.ACCESS_LOG_COLUMN_NUM.getColumnNum());
        Assert.assertEquals(11, Constants.ELogColumnNum.GMV_LOG_COLUMN_NUM.getColumnNum());
        Assert.assertEquals(7, Constants.ELogColumnNum.COLLECT_LOG_COLUMN_NUM.getColumnNum());
        Assert.assertEquals(6, Constants.ELogColumnNum.CART_LOG_COLUMN_NUM.getColumnNum());
        Assert.assertEquals(18, Constants.ELogColumnNum.HOLOTREE_LOG_COLUMN_NUM.getColumnNum());
    }
}
