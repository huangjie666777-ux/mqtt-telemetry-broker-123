package com.example.mqtt.broker;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** clientId -> 会话 注册表；处理同 ClientID 接管。 */
public final class SessionRegistry {
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    /** 注册新会话；若存在旧会话则返回旧会话（由调用方关闭旧连接）。 */
    public Session register(Session session) {
        return sessions.put(session.clientId(), session);
    }

    /** 仅当当前映射仍是该会话时才移除，避免旧连接清理误删新会话。 */
    public boolean removeIfSame(Session session) {
        return sessions.remove(session.clientId(), session);
    }

    public Session get(String clientId) {
        return sessions.get(clientId);
    }

    public Collection<Session> all() {
        return sessions.values();
    }

    public int size() {
        return sessions.size();
    }
}
