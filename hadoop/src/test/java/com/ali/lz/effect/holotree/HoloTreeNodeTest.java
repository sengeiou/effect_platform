package com.ali.lz.effect.holotree;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.holotree.SourceMeta;

public class HoloTreeNodeTest {

    private HoloTreeNode holoTreeNode;
    private PTLogEntry ptLogEntry;

    @Before
    public void setUp() {
        ptLogEntry = new PTLogEntry();
        holoTreeNode = new HoloTreeNode(ptLogEntry);
    }

    @Test
    public void getPTLogEntry() {
        assertEquals(this.ptLogEntry, this.holoTreeNode.getPtLogEntry());
    }

    @Test
    public void setAndGetParent() {
        HoloTreeNode parent = new HoloTreeNode(new PTLogEntry());
        holoTreeNode.setParent(parent);
        assertEquals(parent, holoTreeNode.getParent());
    }

    @Test
    public void appendAndGetChildren() {

        List<HoloTreeNode> children = new LinkedList<HoloTreeNode>();

        int n = 10;
        for (int i = 0; i < n; i++) {
            HoloTreeNode child = new HoloTreeNode(new PTLogEntry());
            children.add(child);
        }

        for (int i = 0; i < n; i++) {
            holoTreeNode.appendChild(children.get(i));
        }

        List<HoloTreeNode> c = holoTreeNode.getChildren();

        for (int i = 0; i < n; i++) {
            assertEquals(children.get(i), c.get(i));
        }
    }

    @Test
    public void addAndGetAndInhreitSources() {
        List<String> sources = new LinkedList<String>();

        int n = 10;

        List<SourceMeta> sourceMetas = new LinkedList<SourceMeta>();
        for (int i = 0; i < n; i++) {
            sourceMetas.add(new SourceMeta());
        }

        for (int i = 0; i < n; i++) {
            sources.add(new String("Source" + i));
        }

        for (int i = 0; i < n; i++) {
            holoTreeNode.addSource(sources.get(i), sourceMetas.get(i));
        }

        Map<String, SourceMeta> s = holoTreeNode.getSources();
        assertEquals(n, s.size());
        for (int i = 0; i < n; i++) {
            assertEquals(sourceMetas.get(i), s.get("Source" + i));
        }

        // Inherit sources
        HoloTreeNode child = new HoloTreeNode(new PTLogEntry());
        child.setParent(holoTreeNode);
        child.inheritSources();
        sourceMetas.add(new SourceMeta());
        child.addSource("Source" + n, sourceMetas.get(n));

        s = child.getSources();
        assertEquals(n + 1, s.size());
        for (int i = 0; i < n + 1; i++) {
            assertEquals(sourceMetas.get(i), s.get("Source" + i));
        }
    }

    @Test
    public void setAndGetSerialRootPath() {
        holoTreeNode.setSerialRootPath("SerialRootPath");
        assertEquals(new String("SerialRootPath"), holoTreeNode.getSerialRootPath());
    }

    @Test
    public void setAndGetPTypeRootPath() {
        holoTreeNode.setPTypeRootPath("PTypeRootPath");
        assertEquals(new String("PTypeRootPath"), holoTreeNode.getPTypeRootPath());
    }

    @Test
    public void setAndgetEffectPage() {
        holoTreeNode.setEffectPage(true);
        assertEquals(true, holoTreeNode.isEffectPage());

        holoTreeNode.setEffectPage(false);
        assertEquals(false, holoTreeNode.isEffectPage());
    }
}
