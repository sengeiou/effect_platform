package com.ali.lz.effect.ownership;

import junit.framework.TestCase;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.ownership.etao.EffectETaoTreeRunner;

public class EffectMRRunnerTest extends TestCase {
    private EffectETaoTreeRunner runner;

    @Before
    public void setUp() {
        runner = new EffectETaoTreeRunner();

    }

    @Test
    public void testRmHdfsDirs() {
        // // System.out.println("没事");
        // String[] arg = new String[5];
        // // arg[0] =
        // "/home/feiqiong/workspace/effect_platform/src/test/resources/performancetest/access_input";
        // arg[0] = "/home/feiqiong/tmpdata1";
        // arg[1] = "/home/feiqiong/output";
        // arg[2] = "/home/feiqiong/conf";
        // // arg[2] =
        // "/home/feiqiong/workspace/effect_platform/src/test/resources/report_26.xml,/home/feiqiong/workspace/effect_platform/src/test/resources/report_48.xml,/home/feiqiong/workspace/effect_platform/src/test/resources/report_6.xml";
        // // arg[2] = "/home/feiqiong/report_103.xml";
        // arg[3] = "1";
        // arg[4] = "1";
        // // arg[5] = "/home/feiqiong/mid";
        // // arg[6] = "files=/home/feiqiong/report_103.xml";
        // // arg[6] =
        // "files=/home/feiqiong/workspace/effect_platform/src/test/resources/report_26.xml,/home/feiqiong/workspace/effect_platform/src/test/resources/report_48.xml,/home/feiqiong/workspace/effect_platform/src/test/resources/report_6.xml";
        // // arg[7] = "runner_job=0";
        // try {
        // int success = ToolRunner.run(new EffectETaoTreeRunner(true), arg);
        // assertEquals(success, 0);
        // } catch (Exception e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        String str1 = new String("123144|13131|34543|54353|987080|473766|2233432|098903|238732|93487234|23432432");
        String str2 = new String(
                "12313344|13232131|3423543|54353|98708440|47377766|23423343|09890409|23327749|93459384|23432432");

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            String[] str1List = str1.split("\\|");
            String[] str2List = str2.split("\\|");
            for (String str1Number : str1List) {
                for (String str2Number : str2List) {
                    if (str1Number.equals(str2Number))
                        continue;
                    // if (Integer.valueOf(str1Number).intValue() ==
                    // Integer.valueOf(str2Number).intValue())
                    // continue;
                }
            }
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
