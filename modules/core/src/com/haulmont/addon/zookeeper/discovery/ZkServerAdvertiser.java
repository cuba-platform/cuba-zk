/*
 * Copyright (c) 2008-2017 Haulmont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.haulmont.addon.zookeeper.discovery;

import com.google.common.base.Strings;
import com.haulmont.addon.zookeeper.ZkProperties;
import com.haulmont.cuba.core.app.ServerInfo;
import com.haulmont.cuba.core.sys.AppContext;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * On server start, puts the address of the server to ZooKeeper.
 *
 * @see ZkServerSelector
 */
@Component(ZkServerAdvertiser.NAME)
public class ZkServerAdvertiser implements AppContext.Listener {

    public static final String NAME = "cuba_ServerAdvertiser";

    @Inject
    protected ServerInfo serverInfo;

    protected String localNodePath;

    protected String connection;
    protected String password;
    protected int connectionTimeout;
    protected int sessionTimeout;
    protected int maxRetry;
    protected int retryInterval;

    protected CuratorFramework curator;

    private Logger log = LoggerFactory.getLogger(getClass());

    public ZkServerAdvertiser() {
        AppContext.addListener(this);
    }

    @Override
    public void applicationStarted() {
        initConnectionAddress();
        if (connection == null)
            return;

        initConnectionProperties();
        connect();
        advertise();
    }

    private void initConnectionAddress() {
        String property = AppContext.getProperty("cubazk.connection");
        if (Strings.isNullOrEmpty(property)) {
            property = AppContext.getProperty("jgroups.zkping.connection");
            if (Strings.isNullOrEmpty(property)) {
                log.info("Neither 'cubazk.connection' nor 'jgroups.zkping.connection' properties are not specified, advertiser won't start");
                return;
            }
        }
        connection = property;
    }

    @Override
    public void applicationStopped() {
        if (curator != null) {
            if (localNodePath != null) {
                removeNode(localNodePath);
            }
            curator.close();
        }
    }

    protected void initConnectionProperties() {
        password = ZkProperties.getPassword();
        connectionTimeout = ZkProperties.getConnectionTimeout();
        sessionTimeout = ZkProperties.getSessionTimeout();
        maxRetry = ZkProperties.getMaxRetry();
        retryInterval = ZkProperties.getRetryInterval();
    }

    protected void connect() {
        log.info("Connecting to ZooKeeper at {}", connection);

        if (connection == null || connection.trim().isEmpty()) {
            throw new IllegalArgumentException("Cannot connect to ZooKeeper: missing connection property");
        }

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .ensembleProvider(new FixedEnsembleProvider(connection))
                .connectionTimeoutMs(connectionTimeout)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new RetryNTimes(maxRetry, retryInterval));

        if (password != null && password.length() > 0) {
            builder = builder.authorization("digest", password.getBytes());
        }

        curator = builder.build();
        curator.start();
    }

    protected void advertise() {
        createNodePath();
        byte[] data = serverInfo.getServerId().getBytes(StandardCharsets.UTF_8);
        try {
            if (curator.checkExists().forPath(localNodePath) == null) {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(localNodePath, data);
            } else {
                curator.setData().forPath(localNodePath, data);
            }
            log.debug("Created node {}", localNodePath);
        } catch (Exception e) {
            throw new RuntimeException("Error creating ZooKeeper node", e);
        }
    }

    protected void createNodePath() {
        localNodePath = ZkProperties.ROOT_PATH + "/" + UUID.randomUUID();
    }

    protected void removeNode(String path) {
        try {
            curator.delete().forPath(path);
        } catch (Exception e) {
            log.debug("Cannot remove ZooKeeper node {}: {}", path, e.toString());
        }
    }
}
