package com.ali.lz.effect.ownership;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

import junit.framework.TestCase;

public class LzEffectProtoUtilTest extends TestCase {
    @Before
    public void setUp() {
    }

    @Test
    public void test1() {
        String a = "134001649000.1.2.3.5.6.34.40.41.42.43falsefalsehttp://temai.tmall.com/?spm=3.1000473.197562.2http://www.tmall.com/?spm=3.32730.182962.11017049022oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902426true10310000200003.1000473.197562.2200001340016490134001649010truefalse00-1002147483647truefalse00111.3.85.155Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE-1501016852109_201206181848100";
        TreeNodeValue node = LzEffectProtoUtil.fromString(a);
    }

    public void test2() {
        String a = "13400240030204.205.206.208.209falsefalsehttp://list.tmall.com/search_product.htm?spm=3.274416.251201.71&active=1&from=sn_1_cat&area_code=330100&navlog=3&nav=spu-cat&search_condition=7&style=g&sort=s&n=42&s=0&cat=50101184&prt=1340024001921&prc=1http://temai.tmall.com/?spm=3.1000473.197562.21017049022oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902426true1030200003.1000473.197562.2200001340023997134002399710falsetrue22-1002147483647falsetrue22111.3.85.155Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE130101592914010136041500427853109_201206182053230";
        TreeNodeValue node = LzEffectProtoUtil.fromString(a);
    }
}
