package com.ali.lz.effect.ownership;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.ownership.EffectOwnershipCartNode;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

public class EffectOwnershipCartTreeNodeTest extends TestCase {
    private EffectOwnershipCartNode node;

    @Before
    public void setUp() {
    }

    @Test
    public void test1() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        TreeNodeValue value = build.build();
        node = new EffectOwnershipCartNode(value);
        List<String> r_list = node.toStringList();
        assertEquals(r_list.isEmpty(), true);
    }
}
