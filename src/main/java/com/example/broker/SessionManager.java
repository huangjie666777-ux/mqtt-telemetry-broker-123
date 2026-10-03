package com.example.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 会话注册表：按 ClientID 管理在线会话，处理同 ID 接管。 */
public final class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    /** 注册新会话；同 ClientID 旧连接被接管关闭，且不删除新会话。 */
    public void register(Session session) {
        Session old = sessions.put(session.clientId(), session);
        if (old != null && old.channel() != session.channel()) {
            old.markTakenOver();
            old.channel().close();
            log.info("clientId={} 被新连接接管，关闭旧连接", session.clientId());
        }
    }

    /** 仅当注册表中仍是该会话时才移除，避免旧连接清理误删新会话。 */
    public boolean remove(Session session) {
        return sessions.remove(session.clientId(), session);
    }

    public Collection<Session> all() {
        return sessions.values();
    }
}
