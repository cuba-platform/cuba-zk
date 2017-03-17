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
import com.haulmont.cuba.core.sys.AppContext;
import com.haulmont.cuba.core.sys.remoting.discovery.StickySessionServerSelector;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Server selector obtaining the list of servers from ZooKeeper.
 */
public class ZkServerSelector extends StickySessionServerSelector {

    protected String protocol = "http://";

    protected String connection;
    protected String password;
    protected int connectionTimeout;
    protected int sessionTimeout;
    protected int maxRetry;
    protected int retryInterval;

    protected CuratorFramework curator;
    protected PathChildrenCache cache;

    protected List<String> urls;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public List<String> getUrls() {
        return urls;
    }

    public void init() {
        initConnectionAddress();
        if (connection == null)
            return;

        initConnectionProperties();
        connect();
        initialDiscovery();
        listenToChanges();
    }

    protected void initConnectionAddress() {
        String property = AppContext.getProperty("cubazk.connection");
        if (Strings.isNullOrEmpty(property)) {
            log.warn("Property 'cubazk.connection' is not defined, using static server list");
            urls = new ArrayList<>();
            String baseUrl = AppContext.getProperty("cuba.connectionUrlList");
            if (baseUrl == null) {
                log.error("Property 'cuba.connectionUrlList' is not defined, no servers to connect to");
                return;
            }
            fallbackToUrlList(baseUrl);
        } else {
            connection = property;
        }
    }

    protected void fallbackToUrlList(String baseUrl) {
        String[] strings = baseUrl.split("[,;]");
        for (String string : strings) {
            if (!StringUtils.isBlank(string)) {
                urls.add(string + "/" + servletPath);
            }
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

    protected void initialDiscovery() {
        List<String> list = new ArrayList<>();
        try {
            for (String node : curator.getChildren().forPath(ZkProperties.ROOT_PATH)) {
                String nodePath = ZKPaths.makePath(ZkProperties.ROOT_PATH, node);
                byte[] bytes = curator.getData().forPath(nodePath);
                if (bytes != null) {
                    String url = new String(bytes, StandardCharsets.UTF_8);
                    if (!list.contains(url))
                        list.add(url);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading servers list", e);
        }
        log.info("Discovered servers: {}", list);
        urls = new CopyOnWriteArrayList<>(
                list.stream()
                    .map(this::serverIdToUrl)
                    .collect(Collectors.toList())
        );
    }

    protected void listenToChanges() {
        cache = new PathChildrenCache(curator, ZkProperties.ROOT_PATH, true);
        try {
            cache.start();
        } catch (Exception e) {
            throw new RuntimeException("Error starting PathChildrenCache", e);
        }
        addCacheListener(cache);
    }

    protected String serverIdToUrl(String s) {
        return protocol + s + "/" + servletPath;
    }

    protected void addCacheListener(PathChildrenCache cache) {
        PathChildrenCacheListener listener = (client, event) -> {
            ChildData childData = event.getData();
            switch (event.getType()) {
                case CHILD_ADDED: {
                    log.trace("Node added: {}", ZKPaths.getNodeFromPath(childData.getPath()));
                    if (childData.getData() != null) {
                        String serverId = new String(childData.getData(), StandardCharsets.UTF_8);
                        String url = serverIdToUrl(serverId);
                        if (!urls.contains(url)) {
                            urls.add(url);
                            log.info("Added server {}", serverId);
                        }
                    }
                    break;
                }

                case CHILD_UPDATED: {
                    log.warn("Node changed: {}", ZKPaths.getNodeFromPath(childData.getPath()));
                    break;
                }

                case CHILD_REMOVED: {
                    log.trace("Node removed: {}", ZKPaths.getNodeFromPath(childData.getPath()));
                    if (childData.getData() != null) {
                        String serverId = new String(childData.getData(), StandardCharsets.UTF_8);
                        String url = serverIdToUrl(serverId);
                        if (urls.contains(url)) {
                            urls.remove(url);
                            log.info("Removed server {}", serverId);
                        }
                    }
                    break;
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    public void destroy() {
        if (curator != null) {
            curator.close();
        }
    }
}
