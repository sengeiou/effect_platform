package com.etao.data.ep.accesstree;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.etao.data.ep.accesstree.proto.AplusAccessTreeNodeProtoUtil;
import com.etao.data.ep.accesstree.proto.AplusAccessTreeNodeProto.AplusAccessTreeNodeValue;
import com.etao.data.ep.accesstree.util.AplusAccessTreeStatusCounter;
import com.etao.data.ep.accesstree.util.AplusAccessTreeUtil;
import com.etao.lz.dw.util.TextPair;
import com.taobao.loganalyzer.aplus.Aplus.AplusLog;

public class AplusAccessTreeMapper extends MapReduceBase implements
		Mapper<LongWritable, BytesWritable, TextPair, BytesWritable> {
	/**
	 * 去掉url和refer中包含的#号后面的内容
	 * 
	 * @param refer
	 * @param reporter
	 * @return
	 */
	private String fixupUrlRefer(String refer, Reporter reporter) {
		if (refer.indexOf('#') != -1) {
			refer = refer.substring(0, refer.indexOf('#'));
			reporter.incrCounter(
					AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_URLREFER_CONTAIN_SHARP,
					1);
		}
		return refer;
	}

	@Override
	public void map(LongWritable key, BytesWritable value,
			OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
			throws IOException {

		AplusLog aplusLog;
		try {
			aplusLog = AplusLog.parseFrom(Arrays.copyOf(value.getBytes(),
					value.getLength()));
		} catch (Exception e) {
			reporter.incrCounter(
					AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_LOG_FORMAT_ERROR,
					1);
			return;
		}
		String logType = aplusLog.getLogtype().toStringUtf8();
		// 取出acookie日志，包括正常pv日志和广告iframe日志，黄金令箭日志不需要
		if ("0".equals(logType)) {
			reporter.incrCounter(
					AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_LOG_IFRAME_COUNT,
					1);
		} else if ("1".equals(logType)) {
			reporter.incrCounter(
					AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_LOG_PV_COUNT,
					1);
		} else {
			reporter.incrCounter(
					AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_LOG_HJLJ_COUNT,
					1);
			return;
		}
		// String version = "2.0.0";
		String acookie = aplusLog.getCna().toStringUtf8();
		String session_cookie = aplusLog.getSid().toStringUtf8();
		// String tree_id = "";
		// String node_id = "";
		// String parent_id = "";
		// String parent_list = "";
		String ip = AplusAccessTreeUtil.longToIP((long) aplusLog.getIp());
		String user_agent = aplusLog.getUserAgent().toStringUtf8();
		String url = fixupUrlRefer(aplusLog.getUrl().toStringUtf8(), reporter);
		if ("".equals(url) || url == null) {
			reporter.incrCounter(
					AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_LOG_URL_NULL,
					1);
			return;
		}
		String url_info = AplusAccessTreeUtil.getUrlInfo(aplusLog);
		String lzsession = aplusLog.getLinezingSession().toStringUtf8();
		String user_id = "0".equals(aplusLog.getUid().toStringUtf8()) ? ""
				: aplusLog.getUid().toStringUtf8();
		String nick = aplusLog.getNick().toStringUtf8();
		// String proxy = "";
		// String pvtime = "";
		// String revised_refer = "";
		String ts = String.valueOf(aplusLog.getTime());
		String refer = fixupUrlRefer(aplusLog.getPre().toStringUtf8(), reporter);

		AplusAccessTreeNodeValue.Builder builder = AplusAccessTreeNodeValue
				.newBuilder();

		builder.setAcookie(acookie);
		builder.setSessionCookie(session_cookie);
		builder.setIp(ip);
		builder.setUserAgent(user_agent);
		builder.setUrl(url);
		builder.setUrlInfo(url_info);
		builder.setLzsession(lzsession);
		builder.setUserId(user_id);
		builder.setNick(nick);
		builder.setTs(Long.parseLong(ts));
		builder.setRefer(refer);

		AplusAccessTreeNodeValue node = builder.build();

		byte[] data = AplusAccessTreeNodeProtoUtil.serialize(node);

		// 按Acookie_SessionCookie分组建树
		Text sessionID = new Text(node.getAcookie() + "_"
				+ node.getSessionCookie());
		Text timestamp = new Text(String.valueOf(node.getTs()));
		output.collect(new TextPair(sessionID, timestamp), new BytesWritable(
				data));
	}
}
