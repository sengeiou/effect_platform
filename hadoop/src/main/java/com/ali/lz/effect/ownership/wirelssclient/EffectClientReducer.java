package com.ali.lz.effect.ownership.wirelssclient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.proto.LzEffectClientProtoUtil;
import com.ali.lz.effect.proto.LzEffectClientProto.ClientNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectClientReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {

    private void outputValue(String target, OutputCollector<Text, Text> output, ClientNodeValue owner,
            ClientNodeValue value) {

        ClientNodeValue.Builder builder = LzEffectClientProtoUtil.genBuilder(owner);
        if (target.equals("guideipv")) {
            builder.setGuideIpv(1);
        } else if (target.equals("effectpv")) {
            builder.setEffectPv(1);
        } else if (target.equals("directipv")) {
            builder.setDirectIpv(1);
        } else if (target.equals("direct")) {
            builder.setDirectGmvTradeNum(value.getGmvTradeNum());
            builder.setDirectGmvAmt(value.getGmvTradeAmt());
            builder.setDirectAlipayTradeNum(value.getAlipayTradeNum());
            builder.setDirectAlipayAmt(value.getAlipayTradeAmt());
        } else if (target.equals("guide")) {
            builder.setGuideGmvTradeNum(value.getGmvTradeNum());
            builder.setGuideGmvAmt(value.getGmvTradeAmt());
            builder.setGuideAlipayTradeNum(value.getAlipayTradeNum());
            builder.setGuideAlipayAmt(value.getAlipayTradeAmt());
        }
        try {
            String data = LzEffectClientProtoUtil.toString(builder.build());
            output.collect(new Text(), new Text(data));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 通过flag标记生成key，区分auction_id和shop_id
     * 
     * @param flag
     * @param key
     * @return
     */
    private String genKey(String flag, String key) {
        return flag + Constants.CTRL_A + key;
    }

    /**
     * map更新函数，此map记录了宝贝坑位访问、店铺坑位访问、list坑位是宝贝页的下一跳访问，方便后续效果归属
     * 
     * @param nodeType
     * @param flag
     * @param value
     * @param planMap
     * @return
     */
    private HashMap<String, HashMap<String, ClientNodeValue[]>> updateMap(String nodeType, String flag,
            ClientNodeValue value, HashMap<String, HashMap<String, ClientNodeValue[]>> planMap) {

        String IDKey;
        ClientNodeValue[] nodeList;
        HashMap<String, ClientNodeValue[]> nodeMap;
        // map中区分宝贝页和店铺页的标记key
        if (flag.equals("i")) {
            IDKey = genKey("i", value.getAuctionId());
        } else {
            IDKey = genKey("s", value.getShopId());
        }
        // map中已存在相应记录，直接替换更新
        if (planMap.containsKey(value.getActName()) && planMap.get(value.getActName()).containsKey(IDKey)) {

            nodeMap = planMap.get(value.getActName());
            nodeList = nodeMap.get(IDKey);
            // 宝贝坑位的优先级高，结点信息始终存在数组的第一位
            if (nodeType.equals("item")) {
                nodeList[0] = value;
            } else { // list坑位和店铺坑位的信息存在数组的第二位
                nodeList[1] = value;
            }
            nodeMap.put(IDKey, nodeList);
            planMap.put(value.getActName(), nodeMap);

        } else { // 新记录，map结构中插入新记录
            nodeList = new ClientNodeValue[2];
            if (planMap.containsKey(value.getActName())) {
                nodeMap = planMap.get(value.getActName());
            } else
                nodeMap = new HashMap<String, ClientNodeValue[]>();

            if (nodeType.equals("item")) {
                nodeList[0] = value;
            } else {
                nodeList[1] = value;
            }
            nodeMap.put(IDKey, nodeList);
            planMap.put(value.getActName(), nodeMap);
        }
        return planMap;
    }

    /**
     * 效果归属函数，对于每个ipv和gmv页面，通过上面的map，分别归属到相应的结点上
     * 
     * @param effectType
     * @param value
     * @param planMap
     * @param output
     */
    private void effectOwnership(String effectType, ClientNodeValue value,
            HashMap<String, HashMap<String, ClientNodeValue[]>> planMap, OutputCollector<Text, Text> output,
            Reporter reporter) {

        String keyID;
        ClientNodeValue tempNode;
        ClientNodeValue tempNode1;
        ClientNodeValue tempNode2;
        if (planMap.isEmpty()) {
            reporter.incrCounter(EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_OWNER_NOPLANMAP, 1);
            return;
        }
        // 对每个ipv和gmv页面，遍历map，一个效果在各个活动中都分别归属一次
        for (HashMap<String, ClientNodeValue[]> nodemap : planMap.values()) {
            keyID = genKey("i", value.getAuctionId());
            // 判断该宝贝是否在之前有访问信息
            if (nodemap.containsKey(keyID)
                    && ((effectType.equals("pv") && nodemap.get(keyID)[0] != null) || effectType.equals("gmv"))) {
                tempNode1 = nodemap.get(keyID)[0];
                tempNode2 = nodemap.get(keyID)[1];
                if (effectType.equals("pv")) {
                    reporter.incrCounter(
                            EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_OWNER_IPV_ITEM, 1);
                    outputValue("guideipv", output, tempNode1, value);
                } else if (effectType.equals("gmv")) {
                    reporter.incrCounter(
                            EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_OWNER_GMV_ITEM, 1);
                    if (tempNode1 != null) {
                        outputValue("direct", output, tempNode1, value); // 宝贝坑位的直接成交效果
                    } else if (tempNode2 != null) {
                        outputValue("guide", output, tempNode2, value); // list坑位的引导成交效果
                    }
                }
            } else { // 没有宝贝访问信息时，做店铺级别的归属
                keyID = genKey("s", value.getShopId());
                if (nodemap.containsKey(keyID)) {
                    tempNode1 = nodemap.get(keyID)[0];
                    tempNode2 = nodemap.get(keyID)[1];
                    if (tempNode1 != null && tempNode2 != null) {
                        tempNode = tempNode1.getTs() >= tempNode2.getTs() ? tempNode1 : tempNode2;
                    } else {
                        tempNode = (tempNode1 == null ? tempNode2 : tempNode1);
                    }
                    if (effectType.equals("pv")) {
                        reporter.incrCounter(
                                EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_OWNER_IPV_SHOP, 1);
                        outputValue("guideipv", output, tempNode, value); // 店铺or宝贝坑位的引导pv
                    } else if (effectType.equals("gmv")) {
                        reporter.incrCounter(
                                EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_OWNER_GMV_SHOP, 1);
                        outputValue("guide", output, tempNode, value); // 店铺or宝贝坑位的引导成交效果
                    }
                } else {
                    reporter.incrCounter(EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_OWNER_OTHER,
                            1);
                }
            }
        }
    }

    @Override
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        HashMap<String, HashMap<String, ClientNodeValue[]>> planMap = new HashMap<String, HashMap<String, ClientNodeValue[]>>();

        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            ClientNodeValue value = LzEffectClientProtoUtil.deserializeClientNodeValue(Arrays.copyOf(
                    nodeData.getBytes(), nodeData.getLength()));
            if (value == null) {
                continue;
            }
            if (value.getLogType() == 0) { // 访问日志
                reporter.incrCounter(EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_BROWSE_LOG, 1);
                if (value.getIsEffectPage()) { // 是否是活动页
                    reporter.incrCounter(
                            EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_ACTIVITY_PAGE, 1);
                    outputValue("effectpv", output, value, value);

                } else if (value.getReferIsEffectPage() && value.getPitId().equals("1")
                        && value.getAuctionId().length() > 0) { // 是否是宝贝坑位页面
                    reporter.incrCounter(
                            EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_ITEM_PIT_PAGE, 1);
                    // 直接输出该页面信息，直接ipv置1，作为宝贝坑位的直接ipv，后续通过hive统计总和
                    outputValue("directipv", output, value, value);
                    // 更新map信息，对于宝贝坑位页面，在map中以auctionid和shopid记录2次
                    planMap = updateMap("item", "i", value, planMap);
                    if (value.getShopId().length() > 0) {
                        planMap = updateMap("item", "s", value, planMap);
                    }

                } else if (value.getAuctionId().length() > 0 && value.getPitId().equals("2")) { // 是否是list坑位第一跳宝贝页面
                    reporter.incrCounter(
                            EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_LISTITEM_PIT_PAGE, 1);
                    // 直接输出，作为list坑位的引导ipv
                    outputValue("guideipv", output, value, value);
                    // list坑位把第一跳的宝贝页信息存入map，方便后续成交归属
                    planMap = updateMap("list", "i", value, planMap);

                } else if (value.getReferIsEffectPage() && value.getPitId().equals("3")
                        && value.getShopId().length() > 0) { // 是否是店铺坑位页面
                    reporter.incrCounter(
                            EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_SHOP_PIT_PAGE, 1);
                    // 店铺坑位无需直接输出，只把店铺信息存入map中
                    planMap = updateMap("shop", "s", value, planMap);

                } else if (value.getAuctionId().length() > 0) { // 普通ipv日志，做流量归属
                    reporter.incrCounter(EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_IPV_PAGE, 1);
                    effectOwnership("pv", value, planMap, output, reporter);
                }
            } else if (value.getLogType() == 1) { // 交易日志，做交易归属
                reporter.incrCounter(EffectJobStatusCounter.WirelessOwnershipStatus.WIRELESS_OWNERSHIP_GMV_LOG, 1);
                effectOwnership("gmv", value, planMap, output, reporter);
            }
        }
    }
}
