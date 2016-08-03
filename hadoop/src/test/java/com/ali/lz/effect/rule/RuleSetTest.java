package com.ali.lz.effect.rule;

import java.io.FileNotFoundException;

import org.junit.Test;

public class RuleSetTest {

    @Test
    public void test() throws FileNotFoundException {
        RuleTestUtil.testDriver("rule/rule1.yaml", "case10", false);
    }
}
