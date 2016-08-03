package com.ali.lz.effect.ownership;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.ownership.EffectOwnershipHoloTreeNode;
import com.ali.lz.effect.ownership.EffectOwnershipGmvNode;
import com.ali.lz.effect.ownership.EffectOwnershipPathinfo;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

import junit.framework.TestCase;

public class EffectOwnershipGmvTreeNodeTest extends TestCase {
    private EffectOwnershipGmvNode node;

    @Before
    public void setUp() {
    }

    @Test
    public void test1() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        TreeNodeValue value = build.build();
        node = new EffectOwnershipGmvNode(value);
        List<String> r_list = node.toStringList();
        assertEquals(r_list.isEmpty(), true);
    }

    @Test
    public void test2() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        build.setTs(100000);
        build.setShopId("shop_id");
        build.setAuctionId("auction_id");
        build.setUserId("user_id");
        build.setAliCorp(0);
        build.setGmvAmt(100);
        build.setGmvAuctionNum(11);
        build.setGmvTradeNum(10);
        build.setAlipayAmt(90);
        build.setAlipayAuctionNum(19);
        build.setAlipayTradeNum(9);
        TreeNodeValue value = build.build();
        node = new EffectOwnershipGmvNode(value);

        TreeNodeValue.Builder a_build = TreeNodeValue.newBuilder();
        TreeNodeValue a_value = build.build();
        EffectOwnershipHoloTreeNode a_node = new EffectOwnershipHoloTreeNode(a_value);
        EffectOwnershipPathinfo a_path = new EffectOwnershipPathinfo();
        a_node.plan_id = 1;
        TreeNodeValue.TypeRef.TypePathInfo.Builder ptype_builder = TreeNodeValue.TypeRef.TypePathInfo.newBuilder();
        TreeNodeValue.TypeRef.TypePathInfo path_type = ptype_builder.build();
        a_node.path_infos.add(a_path);
        node.CopyFromHoloTreeNode(a_node);

        node.calcEffects();

    }
}
