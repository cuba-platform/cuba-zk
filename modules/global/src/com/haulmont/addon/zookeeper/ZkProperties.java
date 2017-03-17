package com.haulmont.addon.zookeeper;

import com.haulmont.cuba.core.sys.AppContext;

import javax.annotation.Nullable;

/**
 * Class that load ZooKeeper connection properties from application or system properties.
 * We cannot use configuration interface here because these properties are used before application context is started.
 */
public class ZkProperties {

    public static final String ROOT_PATH = "/cuba/servers";

    public static final int CONNECTION_TIMEOUT = 15 * 1000;
    public static final int SESSION_TIMEOUT = 60 * 1000;
    public static final int MAX_RETRY = 3;
    public static final int RETRY_INTERVAL = 500;

    @Nullable
    public static String getPassword() {
        String property = AppContext.getProperty("cubazk.password");
        return property != null ? property : AppContext.getProperty("jgroups.zkping.password");
    }

    public static int getConnectionTimeout() {
        String property = AppContext.getProperty("cubazk.connectionTimeout");
        if (property == null)
            property = AppContext.getProperty("jgroups.zkping.connection_timeout");
        return property != null ? Integer.parseInt(property) : CONNECTION_TIMEOUT;
    }

    public static int getSessionTimeout() {
        String property = AppContext.getProperty("cubazk.sessionTimeout");
        if (property == null)
            property = AppContext.getProperty("jgroups.zkping.session_timeout");
        return property != null ? Integer.parseInt(property) : SESSION_TIMEOUT;
    }

    public static int getMaxRetry() {
        String property = AppContext.getProperty("cubazk.maxRetry");
        if (property == null)
            property = AppContext.getProperty("jgroups.zkping.max_retry");
        return property != null ? Integer.parseInt(property) : MAX_RETRY;
    }

    public static int getRetryInterval() {
        String property = AppContext.getProperty("cubazk.retryInterval");
        if (property == null)
            property = AppContext.getProperty("jgroups.zkping.retry_interval");
        return property != null ? Integer.parseInt(property) : RETRY_INTERVAL;
    }
}
