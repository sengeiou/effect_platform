package com.ali.lz.effect.ownership.pid;

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
import com.ali.lz.effect.ownership.pid.EffectPidOwnershipUtil.effectType;
import com.ali.lz.effect.proto.LzEffectPidProtoUtil;
import com.ali.lz.effect.proto.LzEffectPidProto.PidNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectPidOwnershipReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {
    /**
     * 直接输出函数
     * 
     * @param target
     * @param output
     * @param owner
     *            效果归属结点
     * @param value
     *            被归属结点
     */
    private void outputValue(EffectPidOwnershipUtil.effectType target, OutputCollector<Text, Text> output,
            PidNodeValue owner, PidNodeValue value) {

        PidNodeValue.Builder builder = LzEffectPidProtoUtil.genBuilder(owner);
        if (target == effectType.effectPv) {
            builder.setEffectPv(1);
            builder.setChannelPv(1);
        } else if (target == effectType.effectClickPv) {
            builder.setEffectClickPv(1);
            builder.setChannelPv(1);
        } else if (target == effectType.channelPv) {
            builder.setChannelPv(1);
        } else if (target == effectType.itemlistGuideIpv) {
            if (value.getReferIsEffectPage()) {
                builder.setEffectClickPv(1);
            }
            builder.setGuideIpv(1);
            int itemType = value.getItemType();
            if (itemType == EffectPidTreeUtil.PidItemType.TMALL_P4P.ordinal()
                    || itemType == EffectPidTreeUtil.PidItemType.TAOBAO_P4P.ordinal()) {
                builder.setP4PClickid(value.getItemClickid());
            } else if (itemType == EffectPidTreeUtil.PidItemType.TMALL_CPS.ordinal()
                    || itemType == EffectPidTreeUtil.PidItemType.TAOBAO_CPS.ordinal()) {
                builder.setTbkClickid(value.getItemClickid());
            }
        } else if (target == effectType.itemlistDirectGmv) {
            builder.setDirectGmvTradeNum(value.getGmvTradeNum());
            builder.setDirectGmvAmt(value.getGmvTradeAmt());
            builder.setDirectAlipayTradeNum(value.getAlipayTradeNum());
            builder.setDirectAlipayAmt(value.getAlipayTradeAmt());
            builder.setOrderId(value.getOrderId());
            int itemType = owner.getItemType();
            if (itemType == EffectPidTreeUtil.PidItemType.TMALL_CPS.ordinal()) {
                builder.setTbkFlag(2);
            } else if (itemType == EffectPidTreeUtil.PidItemType.TAOBAO_CPS.ordinal()) {
                builder.setTbkFlag(1);
            }
        } else if (target == effectType.shopDirectGmv) {
            builder.setDirectGmvTradeNum(value.getGmvTradeNum());
            builder.setDirectGmvAmt(value.getGmvTradeAmt());
            builder.setDirectAlipayTradeNum(value.getAlipayTradeNum());
            builder.setDirectAlipayAmt(value.getAlipayTradeAmt());
            builder.setOrderId(value.getOrderId());
        } else if (target == effectType.itemGuideGmv) {
            builder.setGuideGmvTradeNum(value.getGmvTradeNum());
            builder.setGuideGmvAmt(value.getGmvTradeAmt());
            builder.setGuideAlipayTradeNum(value.getAlipayTradeNum());
            builder.setGuideAlipayAmt(value.getAlipayTradeAmt());
            builder.setOrderId(value.getOrderId());
        }

        try {
            String data = LzEffectPidProtoUtil.toString(builder.build());
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
    private HashMap<String, PidNodeValue[]> updateMap(String nodeType, String flag, PidNodeValue value,
            HashMap<String, PidNodeValue[]> nodeMap) {

        String IDKey;
        PidNodeValue[] nodeList;
        // map中区分宝贝页和店铺页的标记key
        if (flag.equals("i")) {
            IDKey = genKey("i", value.getAuctionId());
        } else {
            IDKey = genKey("s", value.getShopId());
        }
        // map中已存在相应记录，直接替换更新
        if (nodeMap.containsKey(IDKey)) {

            nodeList = nodeMap.get(IDKey);
            // 宝贝坑位的优先级高，结点信息始终存在数组的第一位
            if (nodeType.equals("item")) {
                nodeList[0] = value;
            } else { // list坑位和店铺坑位的信息存在数组的第二位
                nodeList[1] = value;
            }
            nodeMap.put(IDKey, nodeList);

        } else { // 新记录，map结构中插入新记录
            nodeList = new PidNodeValue[2];
            nodeMap = new HashMap<String, PidNodeValue[]>();

            if (nodeType.equals("item")) {
                nodeList[0] = value;
            } else {
                nodeList[1] = value;
            }
            nodeMap.put(IDKey, nodeList);
        }
        return nodeMap;
    }

    /**
     * 效果归属函数，对于每个ipv和gmv页面，通过上面的map，分别归属到相应的结点上
     * 
     * @param effectType
     * @param value
     * @param planMap
     * @param output
     */
    private void effectOwnership(String effectOwnershipType, PidNodeValue value,
            HashMap<String, PidNodeValue[]> nodeMap, OutputCollector<Text, Text> output, Reporter reporter) {

        String keyID;
        PidNodeValue tempNode;
        PidNodeValue tempNode1;
        PidNodeValue tempNode2;
        if (nodeMap.isEmpty()) {
            reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_OWNER_NOPLANMAP, 1);
            return;
        }
        keyID = genKey("i", value.getAuctionId());
        // 判断该宝贝是否在之前有访问信息
        if (nodeMap.containsKey(keyID)) {
            tempNode1 = nodeMap.get(keyID)[0];
            tempNode2 = nodeMap.get(keyID)[1];
            if (effectOwnershipType.equals("pv")) {
                reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_OWNER_IPV_ITEM, 1);
                if (tempNode1 != null) {
                    outputValue(effectType.itemlistGuideIpv, output, tempNode1, value);
                } else {
                    outputValue(effectType.itemlistGuideIpv, output, tempNode2, value);
                }

            } else if (effectOwnershipType.equals("gmv")) {
                reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_OWNER_GMV_ITEM, 1);
                if (tempNode1 != null) {
                    // 宝贝坑位的直接成交效果
                    outputValue(effectType.itemlistDirectGmv, output, tempNode1, value);
                } else if (tempNode2 != null) {
                    // list坑位的直接成交效果
                    outputValue(effectType.itemlistDirectGmv, output, tempNode2, value);
                }
            }
        } else {
            // 没有宝贝访问信息时，做店铺级别的归属
            keyID = genKey("s", value.getShopId());
            if (nodeMap.containsKey(keyID)) {
                tempNode1 = nodeMap.get(keyID)[0];
                tempNode2 = nodeMap.get(keyID)[1];
                if (tempNode1 != null && tempNode2 != null) {
                    tempNode = tempNode1.getTs() >= tempNode2.getTs() ? tempNode1 : tempNode2;
                } else {
                    tempNode = (tempNode1 == null ? tempNode2 : tempNode1);
                }
                if (effectOwnershipType.equals("pv")) {
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_OWNER_IPV_SHOP, 1);
                    // 店铺or宝贝坑位的引导pv
                    outputValue(effectType.itemlistGuideIpv, output, tempNode, value);
                } else if (effectOwnershipType.equals("gmv")) {
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_OWNER_GMV_SHOP, 1);
                    if (tempNode == tempNode1) { // 宝贝坑位引导成交
                        outputValue(effectType.itemGuideGmv, output, tempNode, value);
                    } else { // 店铺坑位直接成交
                        outputValue(effectType.shopDirectGmv, output, tempNode, value);
                    }
                }
            } else {
                reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_OWNER_OTHER, 1);
            }
        }
    }

    @Override
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        HashMap<String, PidNodeValue[]> nodeMap = new HashMap<String, PidNodeValue[]>();

        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            PidNodeValue value = LzEffectPidProtoUtil.deserialize(Arrays.copyOf(nodeData.getBytes(),
                    nodeData.getLength()));
            if (value == null) {
                continue;
            }
            if (value.getLogType() == 0) { // 访问日志
                reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_BROWSE_LOG, 1);
                if (value.getIsEffectPage()) { // 是否是活动页
                    if (value.getIsChannelLp()) {
                        reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_ACTIVITY_PAGE, 1);
                        // 输出本活动页的一跳pv
                        outputValue(effectType.effectPv, output, value, value);
                    } else if (value.getReferIsChannelLp()) {
                        reporter.incrCounter(
                                EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_ACTIVITY_CLICK_PAGE, 1);
                        // 输出来源活动页的二跳pv
                        outputValue(effectType.effectClickPv, output, value, value);
                    } else {
                        // 输出活动页的频道pv
                        outputValue(effectType.channelPv, output, value, value);
                    }
                } else if (value.getReferIsEffectPage() && value.getPitId() == EffectPitType.ITEM_PIT
                        && value.getAuctionId().length() > 0) { // 是否是宝贝坑位页面
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_ITEM_PIT_PAGE, 1);
                    // 直接输出该页面信息，直接ipv置1，作为宝贝坑位的直接ipv，后续通过hive统计总和
                    outputValue(effectType.itemlistGuideIpv, output, value, value);
                    // 更新map信息，对于宝贝坑位页面，在map中以auctionid和shopid记录2次
                    nodeMap = updateMap("item", "i", value, nodeMap);
                    if (value.getShopId().length() > 0) {
                        nodeMap = updateMap("item", "s", value, nodeMap);
                    }

                } else if (value.getReferIsEffectPage() && value.getPitId() == EffectPitType.LIST_PIT
                        && !(value.getAuctionId().length() > 0)) { // 是否是list坑位页面
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_LIST_PIT_PAGE, 1);
                    // list坑位，输出活动页二跳指标
                    outputValue(effectType.effectClickPv, output, value, value);

                } else if (value.getAuctionId().length() > 0 && value.getPitId() == EffectPitType.LIST_PIT) { // 是否是list坑位第一跳宝贝页面
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_LISTITEM_PIT_PAGE, 1);
                    // 直接输出，作为list坑位的引导ipv
                    outputValue(effectType.itemlistGuideIpv, output, value, value);
                    // list坑位把第一跳的宝贝页信息存入map，方便后续成交归属
                    nodeMap = updateMap("list", "i", value, nodeMap);
                } else if (value.getReferIsEffectPage() && value.getPitId() == EffectPitType.SHOP_PIT
                        && value.getShopId().length() > 0) { // 是否是店铺坑位页面
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_SHOP_PIT_PAGE, 1);
                    // shop坑位，输出活动页二跳指标
                    outputValue(effectType.effectClickPv, output, value, value);
                    // 店铺坑位无需直接输出，只把店铺信息存入map中
                    nodeMap = updateMap("shop", "s", value, nodeMap);
                } else if (value.getAuctionId().length() > 0) { // 普通ipv日志，做流量归属
                    reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_IPV_PAGE, 1);
                    effectOwnership("pv", value, nodeMap, output, reporter);
                }
            } else if (value.getLogType() == 1) { // 交易日志，做交易归属
                reporter.incrCounter(EffectJobStatusCounter.PidOwnershipStatus.PID_OWNERSHIP_GMV_LOG, 1);
                effectOwnership("gmv", value, nodeMap, output, reporter);
            }
        }
    }
}