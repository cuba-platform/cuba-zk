/*
 *  Copyright 2005-2014 Red Hat, Inc.
 *
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.haulmont.addon.zookeeper.jgroups;

import com.haulmont.addon.zookeeper.ZkProperties;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;

/**
 * ZooKeeper ping JMX bean.
 */
@MBean(description = "ZooKeeper based discovery protocol. Acts as a ZooKeeper client and accesses ZooKeeper servers to fetch discovery information")
public class ConfigurableZooKeeperPing extends AbstractZooKeeperPing {
    @Property
    protected String connection;

    @Property
    protected String password;

    @Property
    protected int connection_timeout = ZkProperties.CONNECTION_TIMEOUT;

    @Property
    protected int session_timeout = ZkProperties.SESSION_TIMEOUT;

    @Property
    protected int max_retry = ZkProperties.MAX_RETRY;

    @Property
    protected int retry_interval = ZkProperties.RETRY_INTERVAL;

    @Property
    protected int mode = CreateMode.EPHEMERAL.toFlag();

    private ACLProvider aclProvider;

    static {
        ClassConfigurator.addProtocol((short) 1002, ConfigurableZooKeeperPing.class);
    }

    @Override
    protected CreateMode getCreateMode() throws KeeperException {
        return CreateMode.fromFlag(mode);
    }

    @Override
    public void init() throws Exception {
        if (connection == null || connection.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing connection property!");
        }
        super.init();
        curator.start();
    }

    @Override
    public void destroy() {
        try {
            curator.close();
        } finally {
            super.destroy();
        }
    }

    protected String getScheme() {
        return "digest";
    }

    protected byte[] getAuth() {
        return password.getBytes();
    }

    protected CuratorFramework createCurator() throws KeeperException {
        log.info(String.format("Creating curator [%s], mode: %s", connection, getCreateMode()));

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
            .ensembleProvider(new FixedEnsembleProvider(connection))
            .connectionTimeoutMs(connection_timeout)
            .sessionTimeoutMs(session_timeout)
            .retryPolicy(new RetryNTimes(max_retry, retry_interval));

        if (password != null && password.length() > 0) {
            builder = builder.authorization(getScheme(), getAuth()).aclProvider(aclProvider);
        }

        return builder.build();
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connection_timeout = connectionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.session_timeout = sessionTimeout;
    }

    public void setMaxRetry(int maxRetry) {
        this.max_retry = maxRetry;
    }

    public void setRetryInterval(int retryInterval) {
        this.retry_interval = retryInterval;
    }

    public void setAclProvider(ACLProvider aclProvider) {
        this.aclProvider = aclProvider;
    }
}
