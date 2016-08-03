package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.hadooputils.TextPair;

public class EffectHjljOwnershipTest {

    MapDriver<Object, Text, TextPair, Text> access_map;
    MapDriver<Object, Text, TextPair, Text> hjlj_map;
    ReduceDriver<TextPair, Text, Text, Text> reduce;

    @Before
    public void setUp() {
        EffectHjljOwnershipMapper.AccessLogMapper access_mapper = new EffectHjljOwnershipMapper.AccessLogMapper();
        EffectHjljOwnershipMapper.HjljLogMapper hjlj_mapper = new EffectHjljOwnershipMapper.HjljLogMapper();

        EffectHjljOwnershipReducer reducer = new EffectHjljOwnershipReducer();

        access_map = new MapDriver<Object, Text, TextPair, Text>();
        access_map.setMapper(access_mapper);
        hjlj_map = new MapDriver<Object, Text, TextPair, Text>();
        hjlj_map.setMapper(hjlj_mapper);

        reduce = new ReduceDriver<TextPair, Text, Text, Text>();
        reduce.setReducer(reducer);
    }

    @Test
    public void hjljOwnershipAccessMapperTest() {
        String input = "1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318";
        access_map.withInput(new Object(), new Text(input));
        TextPair key = new TextPair(new Text("oQeHBkTHfx8CAULF9HpKkfTf_url"), new Text("1340012598"));
        access_map.withOutput(key, new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        access_map.runTest();

    }
    
    @Test
    public void hjljOwnershipHjljLogMapperTest() {
        String input = "1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318";
        hjlj_map.withInput(new Object(), new Text(input));
        TextPair key = new TextPair(new Text("oQeHBkTHfx8CAULF9HpKkfTf_url"), new Text("1340012598"));
        hjlj_map.withOutput(key, new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        hjlj_map.runTest();
    }

    @Test
    public void singleHjljOwnershipTest1() {
        TextPair key = new TextPair(new Text("oQeHBkTHfx8CAULF9HpKkfTf_url"), new Text("1340012598"));
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withInput(key, values);

        reduce.withOutput(new Text(""), new Text("1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withOutput(new Text(""), new Text("1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withOutput(new Text(""), new Text("1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withOutput(new Text(""), new Text("1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.runTest();
    }

    @Test
    public void singleHjljOwnershipTest2() throws IOException {
        TextPair key = new TextPair(new Text("oQeHBkTHfx8CAULF9HpKkfTf_url"), new Text("1340012598"));
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withInput(key, values);

        Assert.assertTrue(reduce.run().isEmpty());
    }
    
    @Test
    public void singleHjljOwnershipTest3() {
        TextPair key = new TextPair(new Text("oQeHBkTHfx8CAULF9HpKkfTf_url"), new Text("1340012598"));
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("01340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902adid1234111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1111.3.85.155Mozilla/4.0 -112_20120618174318"));
        values.add(new Text("11340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withInput(key, values);

        reduce.withOutput(new Text(""), new Text("1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.withOutput(new Text(""), new Text("1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902logkeylogkey=tmallppz.1.1&action=9&type=1logkey=tmallppz.1.1&action=9&type=1adid1234111.3.85.155Mozilla/4.0 -112_20120618174318"));
        reduce.runTest();
    }
}
