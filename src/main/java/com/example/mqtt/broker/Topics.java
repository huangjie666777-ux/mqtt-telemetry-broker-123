package com.example.mqtt.broker;

import java.nio.charset.StandardCharsets;

/** 主题名/过滤器校验与 MQTT 3.1.1 通配符匹配。 */
public final class Topics {
    private Topics() {
    }

    /** 发布主题名校验：合法 UTF-8、非空、不含通配符、不含 U+0000。 */
    public static boolean isValidPublishTopic(String topic) {
        return isValidUtf8(topic) && !topic.isEmpty()
                && topic.indexOf('+') < 0 && topic.indexOf('#') < 0
                && topic.indexOf('\u0000') < 0;
    }

    /** 订阅过滤器校验：+ 独占一级，# 只能出现在末尾且独占一级。 */
    public static boolean isValidFilter(String filter) {
        if (!isValidUtf8(filter) || filter.isEmpty() || filter.indexOf('\u0000') >= 0) {
            return false;
        }
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String lv = levels[i];
            if (lv.indexOf('#') >= 0 && (!lv.equals("#") || i != levels.length - 1)) {
                return false;
            }
            if (lv.indexOf('+') >= 0 && !lv.equals("+")) {
                return false;
            }
        }
        return true;
    }

    /** 严格 UTF-8：解码后再编码逐字节一致（拒绝非法序列）。 */
    private static boolean isValidUtf8(String s) {
        if (s == null) {
            return false;
        }
        return new String(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8).equals(s);
    }

    /**
     * 过滤器匹配主题名。
     * + 匹配单级，# 匹配剩余多级（含其父级），
     * 以 $ 开头的主题不被以 + 或 # 开头的过滤器匹配。
     */
    public static boolean matches(String filter, String topic) {
        if (topic == null || topic.isEmpty()) {
            return false;
        }
        if (topic.charAt(0) == '$' && (filter.startsWith("+") || filter.startsWith("#"))) {
            return false;
        }
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        for (int i = 0; i < f.length; i++) {
            String lv = f[i];
            if (lv.equals("#")) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!lv.equals("+") && !lv.equals(t[i])) {
                return false;
            }
        }
        return f.length == t.length;
    }
}
