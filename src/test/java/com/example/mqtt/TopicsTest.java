package com.example.mqtt;

import com.example.mqtt.broker.Topics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicsTest {
    @Test
    void publishTopicValidation() {
        assertTrue(Topics.isValidPublishTopic("sensors/a/temp"));
        assertTrue(Topics.isValidPublishTopic("$SYS/status"));
        assertFalse(Topics.isValidPublishTopic("a/+/c"));
        assertFalse(Topics.isValidPublishTopic("a/#"));
        assertFalse(Topics.isValidPublishTopic(""));
        assertFalse(Topics.isValidPublishTopic("a\u0000b"));
    }

    @Test
    void filterValidation() {
        assertTrue(Topics.isValidFilter("#"));
        assertTrue(Topics.isValidFilter("a/+/c"));
        assertTrue(Topics.isValidFilter("a/#"));
        assertTrue(Topics.isValidFilter("+"));
        assertFalse(Topics.isValidFilter("a/#/c"));
        assertFalse(Topics.isValidFilter("a/b#"));
        assertFalse(Topics.isValidFilter("a/+b/c"));
        assertFalse(Topics.isValidFilter(""));
    }

    @Test
    void matching() {
        assertTrue(Topics.matches("a/+", "a/b"));
        assertFalse(Topics.matches("a/+", "a/b/c"));
        assertTrue(Topics.matches("a/#", "a"));
        assertTrue(Topics.matches("a/#", "a/b/c"));
        assertTrue(Topics.matches("#", "a/b"));
        assertTrue(Topics.matches("sport/+/player1", "sport/tennis/player1"));
        assertFalse(Topics.matches("sport/+/player1", "sport/tennis/player2"));
        // $ 主题规则
        assertFalse(Topics.matches("#", "$SYS/broker"));
        assertFalse(Topics.matches("+/monitor", "$SYS/monitor"));
        assertTrue(Topics.matches("$SYS/#", "$SYS/broker"));
        assertTrue(Topics.matches("$SYS/+", "$SYS/monitor"));
    }
}
