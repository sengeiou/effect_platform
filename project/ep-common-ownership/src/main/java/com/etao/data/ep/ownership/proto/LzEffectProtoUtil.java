package com.etao.data.ep.ownership.proto;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.lz.dw.util.Constants;
import com.etao.lz.dw.util.StringUtil;

public class LzEffectProtoUtil {

	public static byte[] serialize(TreeNodeValue node) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try{
			node.writeTo(output);
			return output.toByteArray();
		}catch(Exception e){
		}finally{
			try{
				output.close();
			}catch(Exception e){}
		}
		return null;
	}
	
	public static TreeNodeValue deserialize(byte[] data){
		TreeNodeValue value = null;
		try {
			value =  TreeNodeValue.parseFrom(data);
		} catch (InvalidProtocolBufferException e) {
		}
		return value;
	}
	
	/**
	 * 用在Finder和Owner之间传递数据，所以最后业务指标不需要
	 * @param node
	 * @return
	 */
	public static String toString(TreeNodeValue node) {
		List<String> tokenF = new ArrayList<String>();
		List<String> tokenE = new ArrayList<String>();
		List<String> tokenD = new ArrayList<String>();
		List<String> tokenA = new ArrayList<String>();
		
		tokenA.add(String.valueOf(node.getTs()));
		tokenA.add(String.valueOf(node.getLogType()));
		
		tokenA.add(node.getIndexRootPath());
		tokenA.add(String.valueOf(node.getIsLeaf()));
		tokenA.add(String.valueOf(node.getIsRoot()));
		
		tokenA.add(node.getUrl());
		tokenA.add(node.getRefer());
		tokenA.add(node.getShopId());
		tokenA.add(node.getAuctionId());
		tokenA.add(node.getUserId());
		tokenA.add(String.valueOf(node.getAliCorp()));
		
		tokenA.add(node.getCookie());
		tokenA.add(node.getSession());
		tokenA.add(node.getCookie2());
		
		List<TreeNodeValue.TypeRef> typeRefList = node.getTypeRefList();
		for( TreeNodeValue.TypeRef typeRef : typeRefList ){
			tokenE.add(String.valueOf(typeRef.getAnalyzerId()));
			tokenE.add(String.valueOf(typeRef.getPlanId()));
			
			tokenE.add(String.valueOf(typeRef.getIsMatched()));
			tokenE.add(String.valueOf(typeRef.getRtype()));
			tokenE.add(String.valueOf(typeRef.getPtype()));
			for( TreeNodeValue.KeyValueI source_info : typeRef.getSourceInfoList() ) {
				tokenF.add(source_info.getKey() + Constants.CTRL_G + String.valueOf(source_info.getValue()));
			}
			tokenE.add(StringUtil.join(tokenF, Constants.CTRL_F));
			tokenF.clear();
			for( TreeNodeValue.KeyValueS captured_info : typeRef.getCapturedInfoList() ) {
				tokenF.add(captured_info.getKey() + Constants.CTRL_G + captured_info.getValue());
			}
			tokenE.add(StringUtil.join(tokenF, Constants.CTRL_F));
			tokenF.clear();
			
			for( TreeNodeValue.TypeRef.TypePathInfo pathInfo : typeRef.getPathInfoList() ) {
				tokenF.add(pathInfo.getSrc() + Constants.CTRL_G + 
						pathInfo.getFirstTs() + Constants.CTRL_G + 
						pathInfo.getLastTs() + Constants.CTRL_G + 
						pathInfo.getPriority() + Constants.CTRL_G + 
						pathInfo.getIsEffectPage() + Constants.CTRL_G + 
						pathInfo.getRefIsEffectPage() + Constants.CTRL_G + 
						pathInfo.getFirstGuideJumpNum() + Constants.CTRL_G + 
						pathInfo.getFirstGuideAuctionId() + Constants.CTRL_G + 
						pathInfo.getFirstGuideShopId() + Constants.CTRL_G +
						pathInfo.getLastGuideJumpNum() + Constants.CTRL_G + 
						pathInfo.getLastGuideAuctionId() + Constants.CTRL_G + 
						pathInfo.getLastGuideShopId());
			}
			tokenE.add(StringUtil.join(tokenF, Constants.CTRL_F));
			tokenF.clear();
			tokenD.add(StringUtil.join(tokenE, Constants.CTRL_E));
			tokenE.clear();
		}
		tokenA.add(StringUtil.join(tokenD, Constants.CTRL_D));
		tokenD.clear();
		
		
		for( TreeNodeValue.KeyValueS access_useful_extra : node.getAccessUsefulExtraList() ) {
			tokenF.add(access_useful_extra.getKey() + Constants.CTRL_G + access_useful_extra.getValue());
		}
		tokenA.add(StringUtil.join(tokenF, Constants.CTRL_F));
		tokenF.clear();
		tokenA.add(node.getAccessExtra());
		
		/*tokenA.add(String.valueOf(node.getPv()));
		tokenA.add(String.valueOf(node.getGmvAmt()));
		tokenA.add(String.valueOf(node.getGmvAuctionNum()));
		tokenA.add(String.valueOf(node.getGmvTradeNum()));
		tokenA.add(String.valueOf(node.getAlipayAmt()));
		tokenA.add(String.valueOf(node.getAlipayAuctionNum()));
		tokenA.add(String.valueOf(node.getAlipayTradeNum()));
		tokenA.add(String.valueOf(node.getShopCollectNum()));
		tokenA.add(String.valueOf(node.getItemCollectNum()));
		tokenA.add(String.valueOf(node.getCartNum()));*/
		
		return StringUtil.join(tokenA, Constants.CTRL_A);
	}
	
	/**
	 * 用在Finder和Owner之间传递数据，所以最后业务指标不需要
	 * @param line
	 * @return
	 */
	public static TreeNodeValue fromString(String line) {
        String[] fields = line.split(Constants.CTRL_A, -1);
        
        TreeNodeValue.KeyValueS.Builder access_useful_extra_builder = TreeNodeValue.KeyValueS.newBuilder();
        TreeNodeValue.KeyValueS.Builder captured_info_builder = TreeNodeValue.KeyValueS.newBuilder();
        TreeNodeValue.KeyValueI.Builder source_info_builder = TreeNodeValue.KeyValueI.newBuilder();
        TreeNodeValue.TypeRef.TypePathInfo.Builder tpinfo_builder = TreeNodeValue.TypeRef.TypePathInfo.newBuilder();
        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();
        
        builder.setTs(Long.parseLong(fields[0]));
        builder.setLogType(Integer.parseInt(fields[1]));
        
        builder.setIndexRootPath(fields[2]);
        builder.setIsLeaf(Boolean.parseBoolean(fields[3]));       
        builder.setIsRoot(Boolean.parseBoolean(fields[4]));
        
        builder.setUrl(fields[5]);
        builder.setRefer(fields[6]);
        builder.setShopId(fields[7]);
        builder.setAuctionId(fields[8]);
        builder.setUserId(fields[9]);
        builder.setAliCorp(Integer.parseInt(fields[10]));
        
        builder.setCookie(fields[11]);       
        builder.setSession(fields[12]);
        builder.setCookie2(fields[13]);
        
        String[] types = fields[14].split(Constants.CTRL_D, -1);
        for(int i=0; i<types.length; i++){
        	if(types[i].isEmpty()){
        		continue;
        	}
        	String[] typeref = types[i].split(Constants.CTRL_E, -1);
        	type_builder.setAnalyzerId(Integer.parseInt(typeref[0]));
        	type_builder.setPlanId(Integer.parseInt(typeref[1])); 
        	
        	type_builder.setIsMatched(Boolean.parseBoolean(typeref[2]));        	
        	type_builder.setRtype(Integer.parseInt(typeref[3]));
        	type_builder.setPtype(Integer.parseInt(typeref[4]));
        	String[] source_info = typeref[5].split(Constants.CTRL_F, -1);
        	for(int j=0; j<source_info.length; j++){
        		if(source_info[j].isEmpty()){
        			continue;
        		}
        		String[] kvs = source_info[j].split(Constants.CTRL_G, -1);
        		source_info_builder.setKey(kvs[0]);
        		source_info_builder.setValue(Integer.parseInt(kvs[1]));
        		type_builder.addSourceInfo(source_info_builder);
        	}
        	String[] captured_info = typeref[6].split(Constants.CTRL_F, -1);
        	for(int k=0; k<captured_info.length; k++){
        		if(captured_info[k].isEmpty()){
        			continue;
        		}
        		String[] kvs2 = captured_info[k].split(Constants.CTRL_G, -1);
        		captured_info_builder.setKey(kvs2[0]);
        		captured_info_builder.setValue(kvs2[1]);
        		type_builder.addCapturedInfo(captured_info_builder);
        	}
        	
        	String[] pathinfo = typeref[7].split(Constants.CTRL_F, -1);
        	for(int m=0; m<pathinfo.length; m++){
        		if(pathinfo[m].isEmpty()){
        			continue;
        		}
        		String[] kvs3 = pathinfo[m].split(Constants.CTRL_G, -1);
//        		if (kvs3.length < 12) {
//        			// 暂时对kvs3加保护，避免发现因乱码导致切分出错，任务无法完成。
//        			System.err.println(line);
//        			continue;
//        		}
        		tpinfo_builder.setSrc(kvs3[0]);
        		tpinfo_builder.setFirstTs(Long.parseLong(kvs3[1]));
        		tpinfo_builder.setLastTs(Long.parseLong(kvs3[2]));
        		tpinfo_builder.setPriority(Integer.parseInt(kvs3[3]));
        		tpinfo_builder.setIsEffectPage(Boolean.parseBoolean(kvs3[4]));
        		tpinfo_builder.setRefIsEffectPage(Boolean.parseBoolean(kvs3[5]));
        		tpinfo_builder.setFirstGuideJumpNum(Integer.parseInt(kvs3[6]));
        		tpinfo_builder.setFirstGuideAuctionId(kvs3[7]);
        		tpinfo_builder.setFirstGuideShopId(kvs3[8]);
        		tpinfo_builder.setLastGuideJumpNum(Integer.parseInt(kvs3[9]));
        		tpinfo_builder.setLastGuideAuctionId(kvs3[10]);
        		tpinfo_builder.setLastGuideShopId(kvs3[11]);
        		type_builder.addPathInfo(tpinfo_builder);
        	}
        	builder.addTypeRef(type_builder);
        }
        
        String[] access_useful_extra = fields[15].split(Constants.CTRL_F, -1);
    	for(int k=0; k<access_useful_extra.length; k++){
    		if(access_useful_extra[k].isEmpty()){
    			continue;
    		}
    		String[] kvs2 = access_useful_extra[k].split(Constants.CTRL_G, -1);
    		access_useful_extra_builder.setKey(kvs2[0]);
    		access_useful_extra_builder.setValue(kvs2[1]);
    		builder.addAccessUsefulExtra(access_useful_extra_builder);
    	}
    	builder.setAccessExtra(fields[16]);
        
    	/*builder.setPv(Float.valueOf(fields[17]));
    	builder.setGmvAmt(Float.valueOf(fields[18]));
    	builder.setGmvAuctionNum(Float.valueOf(fields[19]));
    	builder.setGmvTradeNum(Float.valueOf(fields[20]));
    	builder.setAlipayAmt(Float.valueOf(fields[21]));
    	builder.setAlipayAuctionNum(Float.valueOf(fields[22]));
    	builder.setAlipayTradeNum(Float.valueOf(fields[23]));
    	builder.setShopCollectNum(Float.valueOf(fields[24]));
    	builder.setItemCollectNum(Float.valueOf(fields[25]));
    	builder.setCartNum(Float.valueOf(fields[26]));*/
    	
        TreeNodeValue node = builder.build();       
		return node;
	}
	
}
