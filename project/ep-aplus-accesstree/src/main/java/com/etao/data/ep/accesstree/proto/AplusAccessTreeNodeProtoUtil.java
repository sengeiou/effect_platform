package com.etao.data.ep.accesstree.proto;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.etao.data.ep.accesstree.proto.AplusAccessTreeNodeProto.AplusAccessTreeNodeValue;
import com.etao.lz.dw.util.Constants;
import com.etao.lz.dw.util.StringUtil;
import com.etao.lz.effect.HoloTreeNode;
import com.etao.lz.effect.PTLogEntry;

public class AplusAccessTreeNodeProtoUtil {
	/**
	 * 
	 * @param node
	 * @return
	 */
	public static byte[] serialize(AplusAccessTreeNodeValue node) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			node.writeTo(output);
			return output.toByteArray();
		} catch (Exception e) {
		} finally {
			try {
				output.close();
			} catch (Exception e) {
			}
		}
		return null;
	}

	/**
	 * 
	 * @param data
	 * @return
	 */
	public static AplusAccessTreeNodeValue deserialize(byte[] data) {
		AplusAccessTreeNodeValue value = null;
		try {
			value = AplusAccessTreeNodeValue.parseFrom(data);
		} catch (InvalidProtocolBufferException e) {
		}
		return value;
	}

	/**
	 * 
	 * @param node
	 * @return
	 */
	public static String toString(AplusAccessTreeNodeValue node) {

		List<String> tokenA = new ArrayList<String>();
		tokenA.add(node.getVersion());
		tokenA.add(node.getAcookie());
		tokenA.add(node.getSessionCookie());
		tokenA.add(node.getTreeId());
		tokenA.add(node.getNodeId());
		tokenA.add(node.getParentId());
		tokenA.add(node.getParentList());
		tokenA.add(node.getIp());
		tokenA.add(node.getUserAgent());
		tokenA.add(node.getUrl());
		tokenA.add(node.getUrlInfo());
		tokenA.add(node.getLzsession());
		tokenA.add(node.getUserId());
		tokenA.add(node.getNick());
		tokenA.add(node.getProxy());
		tokenA.add(node.getPvtime());
		tokenA.add(node.getRevisedRefer());

		return StringUtil.join(tokenA, Constants.CTRL_A);
	}

	/**
	 * 返回树结点输出builder
	 * @param node
	 * @return
	 */
	public static AplusAccessTreeNodeValue.Builder genBuilder(HoloTreeNode node) {

		AplusAccessTreeNodeValue.Builder builder = AplusAccessTreeNodeValue
				.newBuilder();

		builder.setVersion(String.valueOf(node.getPtLogEntry().get("version")));
		builder.setAcookie(String.valueOf(node.getPtLogEntry().get("acookie")));
		builder.setSessionCookie(String.valueOf(node.getPtLogEntry().get(
				"session_cookie")));
		builder.setTreeId((String.valueOf(node.getPtLogEntry().get("tree_id"))));
		builder.setNodeId(String.valueOf(node.getPtLogEntry().get("node_id")));
		builder.setParentId(String.valueOf(node.getPtLogEntry()
				.get("parent_id")));
		builder.setParentList(String.valueOf(node.getPtLogEntry().get(
				"parent_list")));
		builder.setIp(String.valueOf(node.getPtLogEntry().get("ip")));
		builder.setUserAgent(String.valueOf(node.getPtLogEntry().get(
				"user_agent")));
		builder.setUrl(String.valueOf(node.getPtLogEntry().get("url")));
		builder.setUrlInfo(String.valueOf(node.getPtLogEntry().get("url_info")));
		builder.setLzsession(String.valueOf(node.getPtLogEntry().get(
				"lzsession")));
		builder.setUserId((String.valueOf(node.getPtLogEntry().get("uid"))));
		builder.setNick(String.valueOf(node.getPtLogEntry().get("nick")));
		builder.setProxy(String.valueOf(node.getPtLogEntry().get("proxy")));
		builder.setPvtime(String.valueOf(node.getPtLogEntry().get("pvtime")));
		builder.setRevisedRefer(String.valueOf(node.getPtLogEntry().get(
				"revised_refer")));

		return builder;
	}

	/**
	 * 生成建树结点对象
	 * @param node
	 * @return
	 */
	public static PTLogEntry genLogEntry(AplusAccessTreeNodeValue node) {
		PTLogEntry logEntry = new PTLogEntry();
		// 填充原始日志相关字段信息
		logEntry.put("ts", node.getTs());
		logEntry.put("url", node.getUrl());
		logEntry.put("refer_url", node.getRefer());
		// 填充完整建树结点
		logEntry.put("sid", "");
		logEntry.put("uid", node.getUserId());
		logEntry.put("acookie", node.getAcookie());
		logEntry.put("session_cookie", node.getSessionCookie());
		logEntry.put("ip", node.getIp());
		logEntry.put("user_agent", node.getUserAgent());
		logEntry.put("url_info", node.getUrlInfo());
		logEntry.put("lzsession", node.getLzsession());
		logEntry.put("nick", node.getNick());
		// logEntry.put("proxy", node.getProxy());
		// logEntry.put("pvtime", node.getPvtime());
		// logEntry.put("revised_refer", node.getRevisedRefer());
		return logEntry;
	}

}
