package com.ali.lz.effect.extendutils;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.mock.MockOutputCollector;
import org.apache.hadoop.mrunit.mock.MockReporter;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * 在MRunit的MapDriver基础上增加对多行输入的支持
 * 
 * @author feiqiong.dpf
 * 
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public class MapDriverExt<K1, V1, K2, V2> extends MapDriver<K1, V1, K2, V2> {

    protected List<Pair<K1, V1>> inputs = new ArrayList<Pair<K1, V1>>();

    /**
     * Sets the input to send to the mapper
     * 
     * @param key
     * @param val
     */
    @Override
    public void setInput(final K1 key, final V1 value) {
        clearInput();
        addInput(key, value);
    }

    /**
     * Sets the input to send to the mapper
     * 
     * @param inputRecord
     *            a (key, val) pair
     */
    @Override
    public void setInput(final Pair<K1, V1> inputRecord) {
        setInputKey(inputRecord.getFirst());
        setInputValue(inputRecord.getSecond());

        clearInput();
        addInput(inputRecord);
    }

    /**
     * Adds an input to send to the mapper
     * 
     * @param key
     * @param val
     */
    public void addInput(final K1 key, final V1 val) {
        inputs.add(new Pair<K1, V1>(key, val));
    }

    /**
     * Adds an input to send to the mapper
     * 
     * @param input
     *            a (K, V) pair
     */
    public void addInput(final Pair<K1, V1> input) {
        addInput(input.getFirst(), input.getSecond());
    }

    /**
     * Adds list of inputs to send to the mapper
     * 
     * @param inputs
     *            list of (K, V) pairs
     */
    public void addAll(final List<Pair<K1, V1>> inputs) {
        for (Pair<K1, V1> input : inputs) {
            addInput(input);
        }
    }

    /**
     * Clears the list of inputs to send to the mapper
     */
    public void clearInput() {
        inputs.clear();
    }

    /**
     * Adds output (k, v)* pairs we expect from the Mapper
     * 
     * @param outputRecords
     *            The (k, v)* pairs to add
     */
    public void addAllOutput(final List<Pair<K2, V2>> outputRecords) {
        for (Pair<K2, V2> output : outputRecords) {
            addOutput(output);
        }
    }

    /**
     * Adds an output (k, v) pair we expect from the Mapper
     * 
     * @param outputRecord
     *            The (k, v) pair to add
     */
    @Override
    public void addOutput(final Pair<K2, V2> outputRecord) {
        expectedOutputs.add(returnNonNull(outputRecord));
    }

    /**
     * Adds a (k, v) pair we expect as output from the mapper
     * 
     */
    @Override
    public void addOutput(final K2 key, final V2 val) {
        addOutput(new Pair<K2, V2>(key, val));
    }

    /**
     * Identical to setInput() but returns self for fluent programming style
     * 
     * @return this
     */
    @Override
    public MapDriverExt<K1, V1, K2, V2> withInput(final K1 key, final V1 val) {
        setInput(key, val);
        return this;
    }

    /**
     * Identical to setInput() but returns self for fluent programming style
     * 
     * @param inputRecord
     * @return this
     */
    @Override
    public MapDriverExt<K1, V1, K2, V2> withInput(final Pair<K1, V1> inputRecord) {
        setInput(inputRecord);
        return this;
    }

    /**
     * Identical to addAll() but returns self for fluent programming style
     * 
     * @param inputRecords
     * @return this
     */
    public MapDriverExt<K1, V1, K2, V2> withAll(final List<Pair<K1, V1>> inputRecords) {
        addAll(inputRecords);
        return this;
    }

    /**
     * Works like addOutput(), but returns self for fluent style
     * 
     * @param outputRecord
     * @return this
     */
    @Override
    public MapDriverExt<K1, V1, K2, V2> withOutput(final Pair<K2, V2> outputRecord) {
        addOutput(outputRecord);
        return this;
    }

    /**
     * Functions like addOutput() but returns self for fluent programming style
     * 
     * @return this
     */
    @Override
    public MapDriverExt<K1, V1, K2, V2> withOutput(final K2 key, final V2 val) {
        addOutput(key, val);
        return this;
    }

    /**
     * Functions like addAllOutput() but returns self for fluent programming
     * style
     * 
     * @param outputRecords
     * @return this
     */
    public MapDriverExt<K1, V1, K2, V2> withAllOutput(final List<Pair<K2, V2>> outputRecords) {
        addAllOutput(outputRecords);
        return this;
    }

    @Override
    public List<Pair<K2, V2>> run() throws IOException {
        // handle inputKey and inputVal for backwards compatibility
        if (inputKey != null && inputVal != null) {
            setInput(inputKey, inputVal);
        }

        if (inputs == null || inputs.isEmpty()) {
            throw new IllegalStateException("No input was provided");
        }

        if (getMapper() == null) {
            throw new IllegalStateException("No Mapper class was provided");
        }

        final MockOutputCollector<K2, V2> outputCollector = new MockOutputCollector<K2, V2>(getConfiguration());
        final MockReporter reporter = new MockReporter(MockReporter.ReporterType.Mapper, getCounters());

        if (getMapper() instanceof Configurable) {
            ((Configurable) getMapper()).setConf(getConfiguration());
        }
        getMapper().configure(new JobConf(getConfiguration()));
        for (Pair<K1, V1> pair : inputs) {
            getMapper().map(pair.getFirst(), pair.getSecond(), outputCollector, reporter);
        }

        getMapper().close();
        return outputCollector.getOutputs();
    }

    @Override
    public void runTest(final boolean orderMatters) {
        LOG.debug("Mapping input (" + inputKey + ", " + inputVal + ")");
        try {
            final List<Pair<K2, V2>> outputs = run();
            validate(outputs, orderMatters);
            validate(counterWrapper);
        } catch (final IOException ioe) {
            LOG.error("IOException in mapper", ioe);
            throw new RuntimeException("IOException in mapper: ", ioe);
        }
    }
}
