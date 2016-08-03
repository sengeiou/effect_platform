package com.ali.lz.effect.ownership.etao;

import java.util.regex.Pattern;

import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.proto.LzEffectETaoTreeProtoUtil;
import com.ali.lz.effect.proto.LzEffectETaoTreeProto.ETaoTreeNodeValue;
import com.ali.lz.effect.utils.DomainUtil;
import com.ali.lz.effect.utils.StringUtil;

public class EffectETaoTreeUtil {

    public static Pattern tti_pattern = Pattern.compile("^(http|https)://shop\\.etao\\.com/r-.*tti.*");
    public static Pattern sale_pattern = Pattern.compile("^(http|https)://sale\\.etao\\.com/sale\\.htm.*");
    public static Pattern shop_pattern = Pattern.compile("^(http|https)://shop\\.etao\\.com/redirect\\.htm.*");
    public static Pattern detail_pattern = Pattern.compile("^(http|https)://detail\\.etao\\.com/detail\\.htm.*");

    // etao 跳转服务器会在跳出页和宝贝页中间增加跳转日志，寻找宝贝页真正refer时需要被过滤。
    public static Pattern addedLogsPattern = Pattern.compile("^(http|https)://(detail|shop|ok)\\.etao\\.com.*");
    // etao 频道页引导店铺时增加的中间页url规则
    public static Pattern channelShopUrlPattern = Pattern.compile("^(http|https)://buy\\.etao\\.com/store\\.htm.*");

    /**
     * 
     * @param url
     * @param refer
     * @param srcType
     *            {@link ETaoSourceType}中srcType类型
     * @return etao来源类型实例ETaoSourceType
     */
    public static ETaoSourceType parseSrc(String url, String refer, int srcType) {
        ETaoSourceType sourceType = new ETaoSourceType(srcType);
        String urlParameter = "";
        String[] pidParameters = { "" };
        // 按优先级识别来源类型
        if ((urlParameter = StringUtil.getUrlParameter(url, "tb_lm_id")).length() > 0) {
            sourceType.setSrc_id(ETaoSourceType.TB_LM_ID);
            String[] tb_lm_ids = urlParameter.split("_", 2);
            sourceType.addProperty("site_id", tb_lm_ids[0]);
            if (tb_lm_ids.length == 2) {
                sourceType.addProperty("ad_id", tb_lm_ids[1].split(" ", -1)[0]);
            }
        } else if ((urlParameter = StringUtil.getUrlParameter(url, "apply")).length() > 0) {
            sourceType.setSrc_id(ETaoSourceType.TB_EDM_ID);
            sourceType.addProperty("apply", urlParameter);
            sourceType.addProperty("t_id", StringUtil.getUrlParameter(url, "t_id"));
            sourceType.addProperty("linkname", StringUtil.getUrlParameter(url, "linkname"));
        } else if ((urlParameter = StringUtil.getUrlParameter(url, "tb_sem_site")).length() > 0) {
            sourceType.setSrc_id(ETaoSourceType.TB_SEM_SITE);
        } else if ((urlParameter = StringUtil.getUrlParameter(url, "tb_market_id")).length() > 0) {
            sourceType.setSrc_id(ETaoSourceType.TB_MARKET_ID);
            sourceType.addProperty("tb_market_id", urlParameter.split(" ", -1)[0]);
            sourceType.addProperty("refer_site", DomainUtil.getDomainFromUrl(refer, 2));
        } else if (((urlParameter = StringUtil.getUrlParameter(url, "pid")).length() > 0 || (urlParameter = StringUtil
                .getUrlParameter(url, "refpid")).length() > 0)
                && (pidParameters = urlParameter.split("_", -1)).length >= 4) {
            sourceType.setSrc_id(ETaoSourceType.PID);
            sourceType.addProperty("pub_id", pidParameters[1]);
            sourceType.addProperty("pid_site_id", pidParameters[2]);
            sourceType.addProperty("adzone_id", pidParameters[3]);
        } else if (refer == null || refer.length() < 2) {
            sourceType.setSrc_id(ETaoSourceType.SELF_INPUT);
        } else {
            String refer_domain_level1 = DomainUtil.getDomainFromUrl(refer, 1);
            String refer_domain = "";
            try {
                refer_domain = refer_domain_level1.split("\\.")[0];
            } catch (Exception e) {
                refer_domain = "";
            }
            if (refer_domain.equals("baidu")) {
                sourceType.setSrc_id(ETaoSourceType.SEO);
                sourceType.addProperty("src_domain_name_level1", refer_domain_level1);
                String refer_domain_level2 = DomainUtil.getDomainFromUrl(refer, 2);
                sourceType.addProperty("src_domain_name_level2", refer_domain_level2);
                String keyword = "";
                String recommendDecoding = "GB18030";
                if ("m.baidu.com".equalsIgnoreCase(refer_domain_level2))
                    recommendDecoding = "UTF-8";
                if ((keyword = StringUtil.getUrlKeyword(refer, "wd", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                } else if ((keyword = StringUtil.getUrlKeyword(refer, "word", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                } else if ((keyword = StringUtil.getUrlKeyword(refer, "query", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                } else if ((keyword = StringUtil.getUrlKeyword(refer, "kw", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                } else if ((keyword = StringUtil.getUrlKeyword(refer, "w", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                } else if ((keyword = StringUtil.getUrlKeyword(refer, "phpig", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                } else if ((keyword = StringUtil.getUrlKeyword(refer, "q", recommendDecoding)).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                }
            } else if (refer_domain.equals("google") || refer_domain.equals("youdao") || refer_domain.equals("bing")
                    || refer_domain.equals("yahoo")) {
                sourceType.setSrc_id(ETaoSourceType.SEO);
                sourceType.addProperty("src_domain_name_level1", refer_domain_level1);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
                String keyword = "";
                if ((keyword = StringUtil.getUrlKeyword(refer, "q", "UTF-8")).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                }
            } else if (refer_domain.equals("sogou")) {
                sourceType.setSrc_id(ETaoSourceType.SEO);
                sourceType.addProperty("src_domain_name_level1", refer_domain_level1);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
                String keyword = "";
                if ((keyword = StringUtil.getUrlKeyword(refer, "query", "GB18030")).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                }
            } else if (refer_domain.equals("360") || refer_domain.equals("so")) {
                sourceType.setSrc_id(ETaoSourceType.SEO);
                sourceType.addProperty("src_domain_name_level1", refer_domain_level1);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
                String keyword = "";
                if ((keyword = StringUtil.getUrlKeyword(refer, "q", "UTF-8")).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                }
            } else if (refer_domain.equals("soso")) {
                sourceType.setSrc_id(ETaoSourceType.SEO);
                sourceType.addProperty("src_domain_name_level1", refer_domain_level1);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
                String keyword = "";
                if ((keyword = StringUtil.getUrlKeyword(refer, "w", "GB18030")).length() > 0) {
                    sourceType.addProperty("keyword", keyword);
                }
            } else if (!refer_domain.equals("taobao") && !refer_domain.equals("tmall") && !refer_domain.equals("etao")
                    && refer.length() > 0) {
                sourceType.setSrc_id(ETaoSourceType.OUTSIDE);
                sourceType.addProperty("src_domain_name_level1", refer_domain_level1);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
            } else if (refer_domain.equals("taobao") || DomainUtil.getDomainFromUrl(refer, 2).equals("taobao.etao.com")) {
                sourceType.setSrc_id(ETaoSourceType.TAOBAO);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
            } else if (refer_domain_level1.equals("tmall.com")) {
                sourceType.setSrc_id(ETaoSourceType.TMALL);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
            } else if (refer_domain_level1.equals("etao.com")) {
                sourceType.setSrc_id(ETaoSourceType.ETAO);
                sourceType.addProperty("src_domain_name_level2", DomainUtil.getDomainFromUrl(refer, 2));
            } else
                sourceType.setSrc_id(ETaoSourceType.SELF_INPUT); // 默认来源类型
        }
        return sourceType;
    }

    public static String parseTradeTrackInfo(String url, String refer, String auctionId) {
        // 解析trade_track_info
        String trade_track_info = "";

        // 1. 解析常规URL中的trade_track_info
        if (tti_pattern.matcher(url).find()) {
            trade_track_info = StringUtil.getUrlParameter(url, "tti");
        } else if (sale_pattern.matcher(url).find() || shop_pattern.matcher(url).find()
                || detail_pattern.matcher(url).find()) {
            trade_track_info = StringUtil.getUrlParameter(url, "trade_track_info");
        }

        // 2. 解析短URL中的trade_track_info
        if (trade_track_info.length() <= 3) {
            String tmp = StringUtil.getUrlParameter(url, "item_id");
            String referDomain = DomainUtil.getDomainFromUrl(refer, 1);
            String urlDomain = DomainUtil.getDomainFromUrl(url, 2);
            if (referDomain.equals("etao.com")
                    && (urlDomain.equals("chaoshi.tmall.com") || urlDomain.equals("ju.taobao.com"))
                    && (tmp.length() > 1)) {
                trade_track_info = tmp;
            } else if (referDomain.equals("etao.com") && auctionId.length() > 2) {
                trade_track_info = auctionId;
            }

        }
        return trade_track_info;
    }

    /**
     * 根据访问树结构填充部分标签字段 对于非根节点，需要设置和LP、频道页节点关系的属性并继承LP、频道页节点来源属性
     * 
     * @param node
     * @return
     */
    public static ETaoTreeNodeValue genETaoTreeNodeValue(HoloTreeNode node) {
        HoloTreeNode parentNode = node.getParent();
        // 处理所有非根节点
        if (parentNode != null) {
            // 设置LP相关信息
            setLPInfo(node);

            // 设置频道相关信息
            setChannelInfo(node);

        } else {
            // 处理etao子树根节点
            setRootInfo(node);
        }
        ETaoTreeNodeValue.Builder builder = LzEffectETaoTreeProtoUtil.genETaoTreeNodeBuilder(node);
        return builder.build();
    }

    private static void setRootInfo(HoloTreeNode node) {
        PTLogEntry logEntry = node.getPtLogEntry();
        String url = (String) logEntry.get("url");
        String refer = (String) logEntry.get("refer_url");

        ETaoSourceType sourceType = EffectETaoTreeUtil.parseSrc(url, refer, ETaoSourceType.LP_SRC_TYPE);
        if ((Integer) logEntry.get("lp_src") == 0) {
            putSourceTypeValues(logEntry, sourceType);
        }
        // 所有根节点url为etao的均设为LP页
        // 注意：etao子树中仅存在refer_is_etao或者is_etao的节点，如果refer_is_etao但作为了子树的根节点，而且后续子节点还是etao站内，
        // 这种情况下会出现etao来源的一跳uv < etao总uv (此类场景以在逻辑内)
        if ((Boolean) logEntry.get("is_etao")) {
            logEntry.put("is_lp", true);
            logEntry.put("lp_domain_name", DomainUtil.getDomainFromUrl(url, 2));
            // 处理是根节点的频道页
            if ((Integer) logEntry.get("channel_id") > 0) {
                logEntry.put("is_channel_lp", true);
                sourceType.setSrc_type(ETaoSourceType.CHANNEL_SRC_TYPE);
                putSourceTypeValues(logEntry, sourceType);
            }
        } else {
            logEntry.put("lp_domain_name", DomainUtil.getDomainFromUrl(refer, 2));
            if ((Integer) logEntry.get("ref_channel_id") > 0) {
                sourceType.setSrc_type(ETaoSourceType.CHANNEL_SRC_TYPE);
                putSourceTypeValues(logEntry, sourceType);
            }
        }

        // logEntry.put("ipv_refer_url", StringUtil.getDomainFromUrl(refer, 2));
    }

    /**
     * 设置LP相关信息 需要根据父节点的类型设置不同的LP信息 父节点分为两类情况：1. 父节点为LP节点 2. 父节点非LP节点 (2.1
     * 父节点url为etao、非LP 2.2 父节点url非etao、非LP)
     * 
     * @param node
     */
    private static void setLPInfo(HoloTreeNode node) {
        PTLogEntry logEntry = node.getPtLogEntry();
        HoloTreeNode parentNode = node.getParent();
        // 1. 父节点为LP节点
        if ((Boolean) parentNode.getPtLogEntry().get("is_lp")) {
            logEntry.put("ref_is_lp", true);
            extendParentNodeSrcProperties(logEntry, parentNode, ETaoSourceType.LP_SRC_TYPE);
        } else if ((Boolean) parentNode.getPtLogEntry().get("is_etao")) {
            // 2.1 父节点url为etao、非LP
            extendParentNodeSrcProperties(logEntry, parentNode, ETaoSourceType.LP_SRC_TYPE);
        } else {
            // 2.2 父节点url非etao、非LP，那么当前节点为LP
            String url = (String) logEntry.get("url");
            String refer = (String) logEntry.get("refer_url");
            logEntry.put("is_lp", true);
            logEntry.put("lp_domain_name", DomainUtil.getDomainFromUrl(url, 2));
            // 设置landing page的来源属性
            ETaoSourceType sourceType = EffectETaoTreeUtil.parseSrc(url, refer, ETaoSourceType.LP_SRC_TYPE);
            putSourceTypeValues(logEntry, sourceType);
        }
    }

    /**
     * 设置频道相关信息, 主要有两类信息：频道页二跳信息、频道页来源信息 需要根据父节点的类型设置不同的频道相关信息
     * 
     * @param node
     * @param parentNode
     */
    private static void setChannelInfo(HoloTreeNode node) {
        HoloTreeNode parentNode = node.getParent();
        PTLogEntry logEntry = node.getPtLogEntry();
        int channelId = (Integer) logEntry.get("channel_id");

        // 1. 父节点为频道LP节点, 设置频道二跳信息
        if ((Boolean) parentNode.getPtLogEntry().get("is_channel_lp")) {
            logEntry.put("ref_is_channel_lp", true);
            extendParentNodeSrcProperties(logEntry, parentNode, ETaoSourceType.REF_CHANNEL_SRC_TYPE);
        }

        // 对于宝贝页或店铺页需要跳过跳转页找到其真正的父节点
        if (((String) logEntry.get("auction_id")).length() > 0
                || ((String) logEntry.get("trade_track_info")).length() > 0
                || ((String) logEntry.get("shop_id")).length() > 0) {
            parentNode = preProcessItemShopNode(node, parentNode);
        }

        int parentChannelId = (Integer) parentNode.getPtLogEntry().get("channel_id");

        // 3. 当前节点和父节点为同一频道页, 或父节点为频道页，当前节点非频道页, 直接继承父节点频道来源信息
        if ((channelId > 0 && channelId == parentChannelId) || (channelId == 0 && parentChannelId > 0)) {
            extendParentNodeSrcProperties(logEntry, parentNode, ETaoSourceType.CHANNEL_SRC_TYPE);
        }

    }

    private static HoloTreeNode preProcessItemShopNode(HoloTreeNode node, HoloTreeNode parentNode) {
        PTLogEntry logEntry = node.getPtLogEntry();
        if (addedLogsPattern.matcher((String) logEntry.get("refer_url")).find()) {
            HoloTreeNode lastParentNode = parentNode;
            while (parentNode != null
                    && addedLogsPattern.matcher((String) parentNode.getPtLogEntry().get("url")).find()) {
                lastParentNode = parentNode;
                parentNode = parentNode.getParent();
            }
            if (parentNode == null) {
                return lastParentNode;
            } else if (channelShopUrlPattern.matcher((String) parentNode.getPtLogEntry().get("url")).find()
                    && parentNode.getParent() != null
                    && (Integer) parentNode.getParent().getPtLogEntry().get("channel_id") > 0) {
                // 对于父节点url为频道店铺引导中间页时，继承祖父节点的来源信息，并将当前节点auction_id置空，作为店铺引导效果
                logEntry.put("ref_channel_id", parentNode.getParent().getPtLogEntry().get("channel_id"));
                logEntry.put("auction_id", "");
                return parentNode.getParent();
            } else {
                logEntry.put("ref_channel_id", parentNode.getPtLogEntry().get("channel_id"));
                return parentNode;
            }
        }
        return parentNode;

    }

    public static void putSourceTypeValues(PTLogEntry logEntry, ETaoSourceType sourceType) {
        switch (sourceType.getSrc_type()) {
        case ETaoSourceType.LP_SRC_TYPE:
            logEntry.put("lp_src", sourceType.getSrc_id());
            logEntry.put("lp_tb_market_id", sourceType.getSourceProperty("tb_market_id"));
            logEntry.put("lp_refer_site", sourceType.getSourceProperty("refer_site"));
            logEntry.put("lp_site_id", sourceType.getSourceProperty("site_id"));
            logEntry.put("lp_ad_id", sourceType.getSourceProperty("ad_id"));
            logEntry.put("lp_apply", sourceType.getSourceProperty("apply"));
            logEntry.put("lp_t_id", sourceType.getSourceProperty("t_id"));
            logEntry.put("lp_linkname", sourceType.getSourceProperty("linkname"));
            logEntry.put("lp_pub_id", sourceType.getSourceProperty("pub_id"));
            logEntry.put("lp_pid_site_id", sourceType.getSourceProperty("pid_site_id"));
            logEntry.put("lp_adzone_id", sourceType.getSourceProperty("adzone_id"));
            logEntry.put("lp_src_domain_name_level1", sourceType.getSourceProperty("src_domain_name_level1"));
            logEntry.put("lp_src_domain_name_level2", sourceType.getSourceProperty("src_domain_name_level2"));
            logEntry.put("lp_keyword", sourceType.getSourceProperty("keyword"));
            break;
        case ETaoSourceType.CHANNEL_SRC_TYPE:
            logEntry.put("channel_src", sourceType.getSrc_id());
            logEntry.put("channel_tb_market_id", sourceType.getSourceProperty("tb_market_id"));
            logEntry.put("channel_refer_site", sourceType.getSourceProperty("refer_site"));
            logEntry.put("channel_site_id", sourceType.getSourceProperty("site_id"));
            logEntry.put("channel_ad_id", sourceType.getSourceProperty("ad_id"));
            logEntry.put("channel_apply", sourceType.getSourceProperty("apply"));
            logEntry.put("channel_t_id", sourceType.getSourceProperty("t_id"));
            logEntry.put("channel_linkname", sourceType.getSourceProperty("linkname"));
            logEntry.put("channel_pub_id", sourceType.getSourceProperty("pub_id"));
            logEntry.put("channel_pid_site_id", sourceType.getSourceProperty("pid_site_id"));
            logEntry.put("channel_adzone_id", sourceType.getSourceProperty("adzone_id"));
            logEntry.put("channel_src_domain_name_level1", sourceType.getSourceProperty("src_domain_name_level1"));
            logEntry.put("channel_src_domain_name_level2", sourceType.getSourceProperty("src_domain_name_level2"));
            logEntry.put("channel_keyword", sourceType.getSourceProperty("keyword"));
            break;
        case ETaoSourceType.REF_CHANNEL_SRC_TYPE:
            logEntry.put("ref_channel_src", sourceType.getSrc_id());
            logEntry.put("ref_channel_tb_market_id", sourceType.getSourceProperty("tb_market_id"));
            logEntry.put("ref_channel_refer_site", sourceType.getSourceProperty("refer_site"));
            logEntry.put("ref_channel_site_id", sourceType.getSourceProperty("site_id"));
            logEntry.put("ref_channel_ad_id", sourceType.getSourceProperty("ad_id"));
            logEntry.put("ref_channel_apply", sourceType.getSourceProperty("apply"));
            logEntry.put("ref_channel_t_id", sourceType.getSourceProperty("t_id"));
            logEntry.put("ref_channel_linkname", sourceType.getSourceProperty("linkname"));
            logEntry.put("ref_channel_pub_id", sourceType.getSourceProperty("pub_id"));
            logEntry.put("ref_channel_pid_site_id", sourceType.getSourceProperty("pid_site_id"));
            logEntry.put("ref_channel_adzone_id", sourceType.getSourceProperty("adzone_id"));
            logEntry.put("ref_channel_src_domain_name_level1", sourceType.getSourceProperty("src_domain_name_level1"));
            logEntry.put("ref_channel_src_domain_name_level2", sourceType.getSourceProperty("src_domain_name_level2"));
            logEntry.put("ref_channel_keyword", sourceType.getSourceProperty("keyword"));
            break;
        }
    }

    private static void extendParentNodeSrcProperties(PTLogEntry logEntry, HoloTreeNode parentNode, int srcType) {
        switch (srcType) {
        case ETaoSourceType.LP_SRC_TYPE:
            int lp_src = (Integer) parentNode.getPtLogEntry().get("lp_src");
            logEntry.put("lp_src", lp_src);
            logEntry.put("lp_domain_name", (String) parentNode.getPtLogEntry().get("lp_domain_name"));
            logEntry.put("lp_adid", (String) parentNode.getPtLogEntry().get("lp_adid"));
            logEntry.put("lp_tb_market_id", (String) parentNode.getPtLogEntry().get("lp_tb_market_id"));
            logEntry.put("lp_refer_site", (String) parentNode.getPtLogEntry().get("lp_refer_site"));
            logEntry.put("lp_site_id", (String) parentNode.getPtLogEntry().get("lp_site_id"));
            logEntry.put("lp_ad_id", (String) parentNode.getPtLogEntry().get("lp_ad_id"));
            logEntry.put("lp_apply", (String) parentNode.getPtLogEntry().get("lp_apply"));
            logEntry.put("lp_t_id", (String) parentNode.getPtLogEntry().get("lp_t_id"));
            logEntry.put("lp_linkname", (String) parentNode.getPtLogEntry().get("lp_linkname"));
            logEntry.put("lp_pub_id", (String) parentNode.getPtLogEntry().get("lp_pub_id"));
            logEntry.put("lp_pid_site_id", (String) parentNode.getPtLogEntry().get("lp_pid_site_id"));
            logEntry.put("lp_adzone_id", (String) parentNode.getPtLogEntry().get("lp_adzone_id"));
            logEntry.put("lp_keyword", (String) parentNode.getPtLogEntry().get("lp_keyword"));
            logEntry.put("lp_src_domain_name_level1",
                    (String) parentNode.getPtLogEntry().get("lp_src_domain_name_level1"));
            logEntry.put("lp_src_domain_name_level2",
                    (String) parentNode.getPtLogEntry().get("lp_src_domain_name_level2"));
            break;
        case ETaoSourceType.CHANNEL_SRC_TYPE:
            int channel_src = (Integer) parentNode.getPtLogEntry().get("channel_src");
            logEntry.put("channel_src", channel_src);
            logEntry.put("channel_adid", (String) parentNode.getPtLogEntry().get("channel_adid"));
            logEntry.put("channel_tb_market_id", (String) parentNode.getPtLogEntry().get("channel_tb_market_id"));
            logEntry.put("channel_refer_site", (String) parentNode.getPtLogEntry().get("channel_refer_site"));
            logEntry.put("channel_site_id", (String) parentNode.getPtLogEntry().get("channel_site_id"));
            logEntry.put("channel_ad_id", (String) parentNode.getPtLogEntry().get("channel_ad_id"));
            logEntry.put("channel_apply", (String) parentNode.getPtLogEntry().get("channel_apply"));
            logEntry.put("channel_t_id", (String) parentNode.getPtLogEntry().get("channel_t_id"));
            logEntry.put("channel_linkname", (String) parentNode.getPtLogEntry().get("channel_linkname"));
            logEntry.put("channel_pub_id", (String) parentNode.getPtLogEntry().get("channel_pub_id"));
            logEntry.put("channel_pid_site_id", (String) parentNode.getPtLogEntry().get("channel_pid_site_id"));
            logEntry.put("channel_adzone_id", (String) parentNode.getPtLogEntry().get("channel_adzone_id"));
            logEntry.put("channel_keyword", (String) parentNode.getPtLogEntry().get("channel_keyword"));
            logEntry.put("channel_src_domain_name_level1",
                    (String) parentNode.getPtLogEntry().get("channel_src_domain_name_level1"));
            logEntry.put("channel_src_domain_name_level2",
                    (String) parentNode.getPtLogEntry().get("channel_src_domain_name_level2"));
            break;
        case ETaoSourceType.REF_CHANNEL_SRC_TYPE:
            int ref_channel_src = (Integer) parentNode.getPtLogEntry().get("channel_src");
            logEntry.put("ref_channel_src", ref_channel_src);
            logEntry.put("ref_channel_adid", (String) parentNode.getPtLogEntry().get("channel_adid"));
            logEntry.put("ref_channel_tb_market_id", (String) parentNode.getPtLogEntry().get("channel_tb_market_id"));
            logEntry.put("ref_channel_refer_site", (String) parentNode.getPtLogEntry().get("channel_refer_site"));
            logEntry.put("ref_channel_site_id", (String) parentNode.getPtLogEntry().get("channel_site_id"));
            logEntry.put("ref_channel_ad_id", (String) parentNode.getPtLogEntry().get("channel_ad_id"));
            logEntry.put("ref_channel_apply", (String) parentNode.getPtLogEntry().get("channel_apply"));
            logEntry.put("ref_channel_t_id", (String) parentNode.getPtLogEntry().get("channel_t_id"));
            logEntry.put("ref_channel_linkname", (String) parentNode.getPtLogEntry().get("channel_linkname"));
            logEntry.put("ref_channel_pub_id", (String) parentNode.getPtLogEntry().get("channel_pub_id"));
            logEntry.put("ref_channel_pid_site_id", (String) parentNode.getPtLogEntry().get("channel_pid_site_id"));
            logEntry.put("ref_channel_adzone_id", (String) parentNode.getPtLogEntry().get("channel_adzone_id"));
            logEntry.put("ref_channel_keyword", (String) parentNode.getPtLogEntry().get("channel_keyword"));
            logEntry.put("ref_channel_src_domain_name_level1",
                    (String) parentNode.getPtLogEntry().get("channel_src_domain_name_level1"));
            logEntry.put("ref_channel_src_domain_name_level2",
                    (String) parentNode.getPtLogEntry().get("channel_src_domain_name_level2"));
            break;
        }
    }
}
