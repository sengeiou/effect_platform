package com.ali.lz.effect.ownership;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.ownership.EffectOwnershipNode;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

import junit.framework.TestCase;

public class EffectOwnershipNodeTest extends TestCase {

    private EffectOwnershipNode node;

    @Before
    public void setUp() {
    }

    @Test
    public void test1() {
        node = new EffectOwnershipNode();
        List<String> r_list = node.toStringList();
        assertEquals(r_list.size(), 0);
    }

    @Test
    public void test2() {
        TreeNodeValue.Builder build = TreeNodeValue.newBuilder();
        TreeNodeValue value = build.build();
        node = new EffectOwnershipNode(value);
        List<String> r_list = node.toStringList();
        assertEquals(r_list.size(), 0);
    }

}
