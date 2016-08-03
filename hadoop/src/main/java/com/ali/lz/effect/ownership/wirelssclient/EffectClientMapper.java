package com.ali.lz.effect.ownership.wirelssclient;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.proto.LzEffectClientProtoUtil;
import com.ali.lz.effect.proto.LzEffectClientProto.ClientNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectClientMapper {

    /**
     * map输出函数
     * 
     * @param output
     * @param node
     * @throws IOException
     */
    private static void MapOutput(OutputCollector<TextPair, BytesWritable> output, ClientNodeValue node)
            throws IOException {

        byte[] data = LzEffectClientProtoUtil.serializeClientNodeValue(node);
        Text user_id = new Text(node.getUserId());
        if (String.valueOf(user_id).length() < 1 || String.valueOf(user_id).equals("0")) {
            user_id = new Text(String.valueOf(node.getTs()));
        }
        Text timestamp = new Text(String.valueOf(node.getTs()));
        output.collect(new TextPair(user_id, timestamp), new BytesWritable(data));
    }

    /**
     * 流量日志
     * 
     * @author nanjia.lj
     * 
     */
    public static class AccessMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private static int ACCESS_LOG_COLUMN_NUM = 11;
        private static int EXTRA_FIELDS_NUM = 10;
        HashMap<String, HashMap<String, PageType>> clientConfig = new HashMap<String, HashMap<String, PageType>>();

        public static class PageType {

            HashMap<String, String> act; // 活动页页面标记
            String detail; // 宝贝详情页页面标记
            String shop; // 店铺页页面标记
            String list; // list页页面标记
            String app_version;

            PageType() {
            }

            PageType(String version) {
                app_version = version;
            }
        }

        @Override
        public void configure(JobConf conf) {

            try {
                SAXBuilder builder = new SAXBuilder();
                Document doc = builder.build(EffectClientMapper.AccessMapper.class.getClassLoader()
                        .getResourceAsStream("client_config.xml"));
                Element root = doc.getRootElement(); // clientconf
                for (Object o1 : root.getChildren()) {
                    Element app = (Element) o1; // app
                    String app_key = getValue(app, "app_key");
                    String app_version = getValue(app, "app_version");
                    HashMap<String, PageType> clientMap = new HashMap<String, PageType>();
                    clientMap.put("app_version", new PageType(app_version));
                    for (Object o2 : app.getChildren()) {
                        Element pageview = (Element) o2; // page_view
                        String eventid = getValue(pageview, "eventid");
                        PageType pageType = new PageType();
                        pageType.act = new HashMap<String, String>();
                        for (Object o3 : pageview.getChildren()) {
                            Element type = (Element) o3; // page_type
                            String typeName = type.getName();
                            if (typeName.equals("act")) {
                                pageType.act.put(getValue(type, "page_sign"), getValue(type, "act_type"));
                            } else if (typeName.equals("detail")) {
                                pageType.detail = getValue(type, "page_sign");
                            } else if (typeName.equals("shop")) {
                                pageType.shop = getValue(type, "page_sign");
                            } else if (typeName.equals("list")) {
                                pageType.list = getValue(type, "page_sign");
                            }
                        }
                        clientMap.put(eventid, pageType);
                    }
                    clientConfig.put(app_key, clientMap);
                }
            } catch (JDOMException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private static String getValue(Element e, String name) {
            String value = e.getAttributeValue(name);
            return (value == null) ? "" : value;
        }

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < ACCESS_LOG_COLUMN_NUM) {
                System.out.println("数据错误");
                return;
            }
            String extra = fields[10];
            String[] extraFields = extra.split(Constants.CTRL_B, -1);
            if (extraFields.length < EXTRA_FIELDS_NUM) {
                return;
            }
            String app_key = extraFields[0];
            if (!clientConfig.containsKey(app_key)) {
                return;
            }
            String app_version = extraFields[1];
            if (clientConfig.get(app_key).get("app_version").app_version.compareTo(app_version) > 0) {
                return;
            }
            String eventid = extraFields[2];
            if (!clientConfig.get(app_key).containsKey(eventid)) {
                return;
            }
            PageType pt = clientConfig.get(app_key).get(eventid);
            String ip = extraFields[3];
            String carrier = extraFields[4];
            String resolution = extraFields[5];
            String device_model = extraFields[6];
            // String arg2 = extraFields[7];
            // String args = extraFields[8];

            String ts = fields[0];
            String url = fields[1];
            String refer = fields[2];
            String shop_id = fields[3];
            String auction_id = fields[4];
            String user_id = fields[5];
            String cookie = fields[6];
            boolean is_effect_page = false;
            boolean refer_is_effect_page = false;
            String act_name = "";
            String act_type = "";
            String pit_id = "";
            String pit_detail = "";
            if (pt.act.containsKey(url.toLowerCase())) { // 活动页
                is_effect_page = true;
                act_name = extraFields[9];
                act_type = pt.act.get(url.toLowerCase());
            } else if (pt.detail.equals(url.toLowerCase()) && pt.act.containsKey(refer.toLowerCase())) { // 活动页下一跳是宝贝页
                refer_is_effect_page = true;
                act_name = extraFields[9];
                act_type = pt.act.get(refer.toLowerCase());
                pit_id = "1";
                pit_detail = auction_id;
            } else if (pt.shop.equals(url.toLowerCase()) && pt.act.containsKey(refer.toLowerCase())) { // 活动页下一跳是店铺页
                refer_is_effect_page = true;
                act_name = extraFields[9];
                act_type = pt.act.get(refer.toLowerCase());
                pit_id = "3";
                pit_detail = shop_id;
            } else if (pt.list.equals(url.toLowerCase()) && pt.act.containsKey(refer.toLowerCase())) { // 活动页下一跳是list页
                refer_is_effect_page = true;
                act_name = extraFields[9];
                act_type = pt.act.get(refer.toLowerCase());
                pit_id = "2";
                pit_detail = "";
            } else if (pt.detail.equals(url.toLowerCase())) {
                //
            } else {
                return;
            }

            ClientNodeValue.Builder builder = ClientNodeValue.newBuilder();

            builder.setLogType(0); // 访问日志类型为0
            builder.setTs(Long.parseLong(ts));
            builder.setAppKey(app_key);
            builder.setAppVersion(app_version);
            builder.setEventid(eventid);
            builder.setUrl(url);
            builder.setRefer(refer);
            builder.setAuctionId(auction_id);
            builder.setShopId(shop_id);
            builder.setUserId(user_id);
            builder.setDeviceId(cookie);
            builder.setIp(ip);
            builder.setCarrier(carrier);
            builder.setResolution(resolution);
            builder.setDeviceModel(device_model);
            builder.setIsEffectPage(is_effect_page);
            builder.setReferIsEffectPage(refer_is_effect_page);
            builder.setActName(act_name);
            builder.setActType(act_type);
            builder.setPitId(pit_id);
            builder.setPitDetail(pit_detail);

            ClientNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            if (node.getTs() > 0 && node.getDeviceId().length() > 0) {
                MapOutput(output, node);
            }

        }
    }

    /**
     * 交易日志
     * 
     * @author nanjia.lj
     * 
     */
    public static class GmvMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private static int GMV_LOG_COLUMN_NUM = 13;

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < GMV_LOG_COLUMN_NUM) {
                System.out.println("数据错误");
                return;
            }
            // 判断platformid是否是客户端交易日志
            String platformid = fields[11].split(Constants.CTRL_B, -1)[0];
            String[] platformidkv = platformid.split(Constants.CTRL_C, -1);
            if (!(platformidkv[0].equals("platform_id") && platformidkv[1].equals("0"))) {
                return;
            }
            if (Long.parseLong(fields[0]) < 0) {
                return;
            }

            ClientNodeValue.Builder builder = ClientNodeValue.newBuilder();

            builder.setLogType(1); // 交易日志类型为1
            builder.setTs(Long.parseLong(fields[0]) * 1000);
            builder.setAuctionId(fields[2]);
            builder.setShopId(fields[1]);
            builder.setUserId(fields[3]);
            builder.setGmvTradeNum(Integer.parseInt(fields[5]));
            builder.setGmvTradeAmt(Float.parseFloat(fields[6]));
            builder.setGmvAuctionNum(Integer.parseInt(fields[7]));
            builder.setAlipayTradeNum(Integer.parseInt(fields[8]));
            builder.setAlipayTradeAmt(Float.parseFloat(fields[9]));
            builder.setAlipayAuctionNum(Integer.parseInt(fields[10]));

            ClientNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            if (node.getTs() > 0 && node.getUserId().length() > 0 && node.getAuctionId().length() > 0) {
                MapOutput(output, node);
            }
        }

    }

}
