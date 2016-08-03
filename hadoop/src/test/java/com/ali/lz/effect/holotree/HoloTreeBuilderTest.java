package com.ali.lz.effect.holotree;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ali.lz.effect.holotree.GenericLoader.CsvLogFmtDesc;
import com.ali.lz.effect.holotree.GenericLoader.LogFmtDesc;
import com.ali.lz.effect.holotree.GenericLoader.RawLogFmtDesc;

public class HoloTreeBuilderTest {

    @Test
    public void sanity() throws Exception {
        MiscTestUtil.treeBuilderDriver("treebuilder/sanity_conf.xml", "treebuilder/sanity_log.txt",
                "treebuilder/sanity_out.yaml");
    }

    @Test
    public void sanity1() throws Exception {
        MiscTestUtil.treeBuilderDriver("treebuilder/sanity_conf.xml", "treebuilder/sanity_log.txt",
                "treebuilder/sanity_out1.yaml");
    }

    @Test
    public void test1() throws Exception {
        MiscTestUtil.testDriver("treebuilder/test1.xml", "treebuilder/test1.log", "treebuilder/test1.yaml");
    }

    @Test
    public void test2() throws Exception {
        MiscTestUtil.testDriver("treebuilder/test2.xml", "treebuilder/test2.log", "treebuilder/test2.yaml");
    }

    @Test
    public void realdata1() throws Exception {
        MiscTestUtil.testDriver("treebuilder/realdata1.xml", "treebuilder/realdata1.log", "treebuilder/realdata1.yaml");
    }

    @Test
    public void realdata2() throws Exception {
        MiscTestUtil.testDriver("treebuilder/realdata2.xml", "treebuilder/realdata2.log", "treebuilder/realdata2.yaml");
    }

    /**
     * 类似 SPM 的问题，login.etao.com 的 redirect_url 参数未转义，导致用于识别外投广告来源的标识
     * tb_market_id 在 login.etao.com 这一跳也被识别出来，使其覆盖掉了上一跳的 ptype，从而无法匹配。修正 SPM
     * 问题后应该可用。
     * 
     * @throws Exception
     */
    @Test
    public void realdata3() throws Exception {
        MiscTestUtil.testDriver("treebuilder/realdata3.xml", "treebuilder/realdata3.log", "treebuilder/realdata3.yaml");
    }

    /**
     * 针对 SPM 处理问题的测试用例
     * 
     * 具体问题：SPM 在四淘内大部分页面都存在，故几乎每跳的 rtype 都会识别为 SPM 页面类型 103，按照之前构造页面类型 root
     * path 的算法，后一跳的 rtype 非 0 时即会在构造时用其替换上一跳的页面类型，使得用户设置的中间页上存在 SPM 时就存在问题。
     * 
     * 例如规则设置为：<code>103[e=":spm"]->10001->10000</code>，这里 10001 为 search
     * 页类型，10000 为宝贝页类型。
     * 
     * 路径树为： <code>
     * digraph G {
     *     taobao->search[label="spm"]->item1[label="spm"]
     * }
     * </code>
     * 
     * 本来期望匹配 item1 节点，但实际上 search 和 item1 节点的页面类型 root path 分别为 103.10001 和
     * 103.103.10000，没有可匹配的路径。这是因为 item1 节点由于 spm 参数的存在而识别出非 0 的
     * rtype，从而覆盖上一跳的页面类型所致。
     * 
     * @throws Exception
     */
    @Test
    public void spm1() throws Exception {
        MiscTestUtil.testDriver("treebuilder/spm1.xml", "treebuilder/spm1.log", "treebuilder/spm1.yaml");
    }

    @Test
    public void spm2() throws Exception {
        MiscTestUtil.testDriver("treebuilder/spm2.xml", "treebuilder/spm2.log", "treebuilder/spm2.yaml");
    }

    @Test
    public void spm() throws Exception {
        MiscTestUtil.testDriver("treebuilder/spm.xml", "treebuilder/spm.log", "treebuilder/spm.yaml");
    }

    @Test
    public void indicator4() throws Exception {
        MiscTestUtil.testDriver("treebuilder/indtest4.xml", "treebuilder/indtest4.log", "treebuilder/indtest4.yaml");
    }

    @Test
    public void wildmatch() throws Exception {
        MiscTestUtil.testDriver("treebuilder/wildmatch.xml", "treebuilder/wildmatch.log", "treebuilder/wildmatch.yaml");
    }

    @Test
    public void continuousUnknownPage() throws Exception {
        MiscTestUtil.testDriver("treebuilder/contunknown.xml", "treebuilder/contunknown.log",
                "treebuilder/contunknown.yaml");
    }

    @Test
    public void b2cTest2() throws Exception {
        MiscTestUtil.testDriver("treebuilder/b2c_source.xml", "treebuilder/b2c_20120731_2.log",
                "treebuilder/b2c_20120731_2.yaml");
    }

    @Test
    public void b2cTest3() throws Exception {
        MiscTestUtil.testDriver("treebuilder/b2c_source.xml", "treebuilder/b2c_20120731_3.log",
                "treebuilder/b2c_20120731_3.yaml");
    }

    @SuppressWarnings("serial")
    @Test
    public void marketTest1() throws Exception {
        LogFmtDesc fmtDesc = new CsvLogFmtDesc("utf-8", 1);
        // version,mid,sid,tree_id,node_id,parent_id,parent_list,ip,user_agent,url,url_refer,uid,nick,proxy,pvtime,revised_refer,shop_id,adid,stay_time,
        // query,query_pre,uid_mid,search_cat,search_cat_pre,auctionid,auctionid_pre,bucketid,dt
        Map<Integer, String> idxToPb = new HashMap<Integer, String>() {
            {
                put(1, "mid");
                put(2, "sid");
                put(7, "ip");
                put(8, "agent");
                put(9, "url");
                put(11, "uid");
                put(14, "ts");
                put(15, "refer_url");
                put(16, "shopid");
                put(17, "adid");
                put(21, "uid_mid");
                put(24, "auctionid");
            }
        };
        MiscTestUtil.testDriverEx("treebuilder/b2c_source.xml", "treebuilder/market1.csv", "treebuilder/market1.yaml",
                fmtDesc, idxToPb);
    }

    @SuppressWarnings("serial")
    @Test
    public void multitest1() throws Exception {
        LogFmtDesc fmtDesc = new RawLogFmtDesc("\001", "utf-8");
        /**
         * <pre>
         * [0] time_stamp           bigint comment '访问时间戳，到秒',
         * [1] url                  string comment 'url',
         * [2] refer_url            string comment 'refer_url',
         * [3] shop_id              string comment '店铺ID：如果当前访问为店铺内页面，则填写。否则为空字符串',
         * [4] auction_id           string comment '宝贝ID: 如果当前访问为宝贝页面，则填写。否则为空字符串',
         * [5] user_id              string comment '访客ID',
         * [6] cookie               string comment 'cookie用来标识一个访问用户（atpanle日志中用mid）',
         * [7] session              string comment '一次会话的标识（atpanle日志中用sid）',
         * [8] cookie2              string comment '用来计算uv使用（atpanle日志中用mid_uid, 其他情况直接使用cookie）',
         * [9] useful_extra         string comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且?>??属计算中需要? 用字? ',
         * [10] extra                string comment '扩展字段，使用ctrl+B分割。为需要携带字段'
         * </pre>
         * */
        Map<Integer, String> idxToPb = new HashMap<Integer, String>() {
            {
                put(0, "ts");
                put(1, "url");
                put(2, "refer_url");
                put(3, "shopid");
                put(4, "auctionid");
                put(5, "uid");
                put(7, "sid");
            }
        };
        MiscTestUtil.testDriverEx("treebuilder/report_149.xml", "treebuilder/report_149.txt",
                "treebuilder/report_149.yaml", fmtDesc, idxToPb);
    }

    @SuppressWarnings("serial")
    @Test
    public void marketTest2() throws Exception {
        CsvLogFmtDesc fmtDesc = new CsvLogFmtDesc("utf-8", 1);
        // XXX: 原始 CSV 文件中 url 字符串里有 \xHH 这样的内容, 而默认 CSV 字符串转义字符就是 '\',
        // 故会导致载入内容同原始内容不同, 这里修改 CSV 转义字符为原文件中不存在的内容以避免该问题发生
        fmtDesc.escape = '\u1000';
        // version,mid,sid,tree_id,node_id,parent_id,parent_list,ip,user_agent,url,url_refer,uid,nick,proxy,pvtime,revised_refer,shop_id,adid,stay_time,
        // query,query_pre,uid_mid,search_cat,search_cat_pre,auctionid,auctionid_pre,bucketid,dt
        Map<Integer, String> idxToPb = new HashMap<Integer, String>() {
            {
                put(2, "ts");
                put(3, "url");
                put(4, "refer_url");
                put(5, "uid_mid");
                put(6, "shopid");
                put(7, "auctionid");
                put(8, "ip");
                put(9, "mid");
                put(10, "uid");
                put(11, "sid");
                put(13, "agent");
                put(14, "adid");
            }
        };
        MiscTestUtil.testDriverEx("treebuilder/b2c_source.xml", "treebuilder/market2.csv", "treebuilder/market2.yaml",
                fmtDesc, idxToPb);
    }

    @Test
    public void noPathMatchTest() throws Exception {
        MiscTestUtil.testDriver("treebuilder/b2c_source.xml", "treebuilder/b2c_20120731_2.log",
                "treebuilder/b2c_20120731_2n.yaml", false);
    }

    @Test
    public void testData() throws Exception {
        LogFmtDesc fmtDesc = new RawLogFmtDesc("\001", "utf-8");
        /**
         * <pre>
         * [0] time_stamp           bigint comment '访问时间戳，到秒',
         * [1] url                  string comment 'url',
         * [2] refer_url            string comment 'refer_url',
         * [3] shop_id              string comment '店铺ID：如果当前访问为店铺内页面，则填写。否则为空字符串',
         * [4] auction_id           string comment '宝贝ID: 如果当前访问为宝贝页面，则填写。否则为空字符串',
         * [5] user_id              string comment '访客ID',
         * [6] cookie               string comment 'cookie用来标识一个访问用户（atpanle日志中用mid）',
         * [7] session              string comment '一次会话的标识（atpanle日志中用sid）',
         * [8] cookie2              string comment '用来计算uv使用（atpanle日志中用mid_uid, 其他情况直接使用cookie）',
         * [9] useful_extra         string comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且?>??属计算中需要? 用字? ',
         * [10] extra                string comment '扩展字段，使用ctrl+B分割。为需要携带字段'
         * </pre>
         * */
        @SuppressWarnings("serial")
        Map<Integer, String> idxToPb = new HashMap<Integer, String>() {
            {
                put(0, "ts");
                put(1, "url");
                put(2, "refer_url");
                put(3, "shopid");
                put(4, "auctionid");
                put(5, "uid");
                put(7, "sid");
            }
        };
        MiscTestUtil.testDriverEx("treebuilder/report_149.xml", "treebuilder/testdata.log",
                "treebuilder/testdata.yaml", fmtDesc, idxToPb);
    }

}
