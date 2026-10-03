package com.example.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicsTest {

    @Test
    void publishTopicValidation() {
        assertTrue(Topics.isValidPublishTopic("sensor/room1/temp"));
        assertFalse(Topics.isValidPublishTopic("sensor/+/temp"));
        assertFalse(Topics.isValidPublishTopic("sensor/#"));
        assertFalse(Topics.isValidPublishTopic(""));
    }

    @Test
    void filterValidation() {
        assertTrue(Topics.isValidFilter("sensor/+/temp"));
        assertTrue(Topics.isValidFilter("sensor/#"));
        assertTrue(Topics.isValidFilter("#"));
        assertFalse(Topics.isValidFilter("sensor/#/temp"));
        assertFalse(Topics.isValidFilter("sen#sor/temp"));
        assertFalse(Topics.isValidFilter("sensor/t+mp"));
    }

    @Test
    void matching() {
        assertTrue(Topics.matches("sensor/#", "sensor/room1/temp"));
        assertTrue(Topics.matches("sensor/+/temp", "sensor/room1/temp"));
        assertFalse(Topics.matches("sensor/+/temp", "sensor/room1/hum"));
        assertFalse(Topics.matches("sensor/+", "sensor/room1/temp"));
        assertTrue(Topics.matches("sport/+", "sport/"));
        assertFalse(Topics.matches("#", "$SYS/status"));
        assertTrue(Topics.matches("$SYS/#", "$SYS/status"));
        assertTrue(Topics.matches("#", "finance/stock"));
    }
}
