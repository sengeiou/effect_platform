package com.etao.data.ep.ownership;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.etao.data.ep.ownership.proto.LzEffectProtoUtil;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.data.ep.ownership.util.EffectOwnershipStatusCounter;
import com.etao.lz.dw.util.TextPair;
import com.etao.lz.dw.util.Constants;

public class EffectOwnershipMapper {

	private static void MapOutput(
			OutputCollector<TextPair, BytesWritable> output,
			TreeNodeValue node, Reporter reporter) throws IOException {
		byte[] data = LzEffectProtoUtil.serialize(node);
		Text group_id = new Text(node.getUserId() + "_" + node.getAuctionId());
		reporter.incrCounter(
				EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_OUTPUT_TOTAL,
				1);
		if (group_id.toString().equals("_")) {
			reporter.incrCounter(
					EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_OUTPUT_REPLACE_NULL,
					1);
			if ("".equals(String.valueOf(node.getTs()))
					|| String.valueOf(node.getTs()) == null) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_OUTPUT_TS_NULL,
						1);
			}
			// 如果为空，给时间戳，主要为了解决单点问题
			group_id = new Text(String.valueOf(node.getTs()) + "_"
					+ String.valueOf(node.getTs()));
		} else if (group_id.toString().equals(node.getUserId() + "_")) {
			reporter.incrCounter(
					EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_AUCID_NULL,
					1);
			group_id = new Text(node.getUserId() + "_" + node.getTs());
		} else if (group_id.toString().equals("_" + node.getAuctionId())) {
			reporter.incrCounter(
					EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_USRID_NULL,
					1);
			group_id = new Text(node.getTs() + "_" + node.getAuctionId());
		}
		Text timestamp = new Text(String.valueOf(node.getTs()));
		output.collect(new TextPair(group_id, timestamp), new BytesWritable(
				data));
	}

	public static class AccessMapper extends MapReduceBase implements
			Mapper<Object, Text, TextPair, BytesWritable> {

		private static int ACCESS_LOG_COLUMN_NUM = 17;

		@Override
		public void map(Object key, Text value,
				OutputCollector<TextPair, BytesWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			String[] fields = line.split(Constants.CTRL_A, -1);
			if (fields.length < ACCESS_LOG_COLUMN_NUM) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_ACCESS_COLUMN_NUM_ERROR,
						1);
				System.out.println("数据错误");
				return;
			}

			TreeNodeValue node = null;
			try {
				node = LzEffectProtoUtil.fromString(line);
			} catch (Exception e) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_ACCESS_FROMSTRING_ERROR,
						1);
				// 暂时加保护，避免发现因乱码导致切分出错，任务无法完成。
				System.err.println(line);
				return;
			}
			if (node == null) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_ACCESS_NULL_ERROR,
						1);
				return;
			}
			// 如果有染色，才output
			List<TreeNodeValue.TypeRef> type_ref_list = node.getTypeRefList();
			if (type_ref_list.isEmpty()) {
			} else if (type_ref_list.size() > 1) {
				System.err.println("find one type_ref more than 1!");
			} else {
				TreeNodeValue.TypeRef type_ref = type_ref_list.get(0);
				if (type_ref.getPathInfoCount() > 0) {
					MapOutput(output, node, reporter);
				}
			}
		}
	}

	public static class GmvMapper extends MapReduceBase implements
			Mapper<Object, Text, TextPair, BytesWritable> {

		private static int GMV_LOG_COLUMN_NUM = 11;

		@Override
		public void map(Object key, Text value,
				OutputCollector<TextPair, BytesWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			String[] fields = line.split(Constants.CTRL_A, -1);
			if (fields.length < GMV_LOG_COLUMN_NUM) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_GMV_COLUMN_NUM_ERROR,
						1);
				System.out.println("数据错误");
				return;
			}

			TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

			builder.setLogType(1); // gmv日志类型为1
			builder.setTs(Long.parseLong(fields[0]));
			builder.setShopId(fields[1]);
			builder.setAuctionId(fields[2]);
			builder.setUserId(fields[3]);
			builder.setAliCorp(Integer.parseInt(fields[4]));

			builder.setGmvTradeNum(Float.parseFloat(fields[5]));
			builder.setGmvAmt(Float.parseFloat(fields[6]));
			builder.setGmvAuctionNum(Float.parseFloat(fields[7]));
			builder.setAlipayTradeNum(Float.parseFloat(fields[8]));
			builder.setAlipayAmt(Float.parseFloat(fields[9]));
			builder.setAlipayAuctionNum(Float.parseFloat(fields[10]));

			// TreeNode build
			TreeNodeValue node = builder.build();
			if (node == null) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_GMV_NULL_ERROR,
						1);
				return;
			}

			if (node.getTs() > 0 && node.getUserId().length() > 0
					&& node.getAuctionId().length() > 0) {
				MapOutput(output, node, reporter);
			}
		}

	}

	public static class CollectMapper extends MapReduceBase implements
			Mapper<Object, Text, TextPair, BytesWritable> {

		// TODO: 店铺收藏计算可能会有问题？还需要评估下
		private static int COLLECT_LOG_COLUMN_NUM = 9;

		@Override
		public void map(Object key, Text value,
				OutputCollector<TextPair, BytesWritable> output,
				Reporter reporter) throws IOException {

			String line = value.toString();
			String[] fields = line.split(Constants.CTRL_A, -1);
			if (fields.length < COLLECT_LOG_COLUMN_NUM) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_COLLECT_COLUMN_NUM_ERROR,
						1);
				System.out.println("数据错误");
				return;
			}

			TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

			builder.setLogType(2); // 收藏日志类型
			builder.setTs(Long.parseLong(fields[0]));
			builder.setShopId(fields[2]);
			builder.setAuctionId(fields[3]);
			builder.setUserId(fields[4]);
			builder.setAliCorp(Integer.parseInt(fields[5]));

			int type = Integer.parseInt(fields[1]);
			if (type == 0) {
				builder.setShopCollectNum(Float.parseFloat(fields[6]));
			} else if (type == 1) {
				builder.setItemCollectNum(Float.parseFloat(fields[6]));
			}

			// TreeNode build
			TreeNodeValue node = builder.build();
			if (node == null) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_COLLECT_NULL_ERROR,
						1);
				return;
			}

			if (node.getTs() > 0 && node.getUserId().length() > 0
					&& node.getAuctionId().length() > 0) {
				MapOutput(output, node, reporter);
			}

		}
	}

	public static class CartMapper extends MapReduceBase implements
			Mapper<Object, Text, TextPair, BytesWritable> {

		private static int COLLECT_LOG_COLUMN_NUM = 6;

		@Override
		public void map(Object key, Text value,
				OutputCollector<TextPair, BytesWritable> output,
				Reporter reporter) throws IOException {

			String line = value.toString();
			String[] fields = line.split(Constants.CTRL_A, -1);
			if (fields.length < COLLECT_LOG_COLUMN_NUM) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_CART_COLUMN_NUM_ERROR,
						1);
				System.out.println("数据错误");
				return;
			}

			TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

			builder.setLogType(3);
			builder.setTs(Long.parseLong(fields[0]));
			builder.setShopId(fields[1]);
			builder.setAuctionId(fields[2]);
			builder.setUserId(fields[3]);
			builder.setAliCorp(Integer.parseInt(fields[4]));
			builder.setCartNum(Float.parseFloat(fields[5]));

			// TreeNode build
			TreeNodeValue node = builder.build();
			if (node == null) {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_MAP_CART_NULL_ERROR,
						1);
				return;
			}

			if (node.getTs() > 0 && node.getUserId().length() > 0
					&& node.getAuctionId().length() > 0) {
				MapOutput(output, node, reporter);
			}
		}
	}
}
