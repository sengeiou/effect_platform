package com.ali.lz.effect.ownership;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.ownership.EffectOwnershipHoloTreeNode;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;

import junit.framework.TestCase;

public class EffectOwnershipAccessTreeNodeTest extends TestCase {
    private EffectOwnershipHoloTreeNode node;

    @Before
    public void setUp() {
    }

    @Test
    public void test1() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        TreeNodeValue value = build.build();
        node = new EffectOwnershipHoloTreeNode(value);
        List<String> r_list = node.toStringList();
        assertEquals(r_list.isEmpty(), true);
    }

    @Test
    public void test2() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        TreeNodeValue value = build.build();
        node = new EffectOwnershipHoloTreeNode(value);

        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.TypeRef type_ref = type_builder.build();
        node.setPlanInfo(type_ref, "last");

        List<String> r_list = node.toStringList();
        assertEquals(r_list.isEmpty(), true);
    }

    @Test
    public void test3() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        TreeNodeValue value = build.build();
        node = new EffectOwnershipHoloTreeNode(value);

        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.TypeRef type_ref = type_builder.build();
        node.setPlanInfo(type_ref, "last");

        TreeNodeValue.TypeRef.TypePathInfo.Builder ptype_builder = TreeNodeValue.TypeRef.TypePathInfo.newBuilder();
        TreeNodeValue.TypeRef.TypePathInfo path_type = ptype_builder.build();
        node.setPlanPathInfo(path_type);

        List<String> r_list = node.toStringList();
        assertEquals(r_list.size(), 1);

        for (String line : r_list) {
            String[] split = line.split(Constants.CTRL_A);
            assertEquals(split[19], "1.0");
        }
    }

}
