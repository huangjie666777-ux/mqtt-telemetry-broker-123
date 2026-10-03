package com.example.broker;

/** MQTT 3.1.1 主题名/主题过滤器校验与匹配（含 +、# 及 $ 系统主题规则）。 */
public final class Topics {

    private Topics() {
    }

    /** 发布主题名：非空、不含通配符、不含 NUL，UTF-8 已由解码层保证。 */
    public static boolean isValidPublishTopic(String topic) {
        return topic != null && !topic.isEmpty()
                && topic.indexOf('+') < 0 && topic.indexOf('#') < 0
                && topic.indexOf('\u0000') < 0;
    }

    /** 订阅过滤器：非空；# 只能独占最后一级；+ 必须独占一级。 */
    public static boolean isValidFilter(String filter) {
        if (filter == null || filter.isEmpty() || filter.indexOf('\u0000') >= 0) {
            return false;
        }
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String lv = levels[i];
            if (lv.indexOf('#') >= 0) {
                if (!lv.equals("#") || i != levels.length - 1) {
                    return false;
                }
            }
            if (lv.indexOf('+') >= 0 && !lv.equals("+")) {
                return false;
            }
        }
        return true;
    }

    /** 按协议匹配过滤器与主题；不以 $ 开头的过滤器不匹配 $ 开头的主题。 */
    public static boolean matches(String filter, String topic) {
        if (topic == null || topic.isEmpty()) {
            return false;
        }
        if (topic.charAt(0) == '$' && (filter.isEmpty() || filter.charAt(0) != '$')) {
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
            if (lv.equals("+")) {
                continue;
            }
            if (!lv.equals(t[i])) {
                return false;
            }
        }
        return f.length == t.length;
    }
}
