# CUBA ZooKeeper Integration Add-on

## Overview

The add-on is designed for coordinating CUBA cluster using [ZooKeeper](https://zookeeper.apache.org/). 

Standard CUBA implementation of service discovery assumes that there is a static fixed list of available middleware servers. This list must be provided both for inter-server communication mechanism based on JGroups and for clients connecting to the cluster. 

The add-on allows cluster members and clients to communicate through a ZooKeeper instance or ensemble. It enables simple and uniform configuration of middleware and client blocks: they need only the ZooKeeper address to form the cluster and to connect to it. In other words, the add-on provides dynamic service discovery.

## Usage

Select a version of the add-on which is compatible with the platform version used in your project:

| Platform Version | Add-on Version |
| ---------------- | -------------- |
| 6.5.0.RC1        | 0.1.0          |

Add custom application component to your project:

* Artifact group: `com.haulmont.addon.zookeeper`
* Artifact name: `cubazk-global`
* Version: *add-on version*

Launch ZooKeeper on your network. Further, it is assumed that the ZooKeeper address is `192.168.0.1:2181`.

Configure your middleware and client blocks using application properties as described below. You can specify default values of the properties in a project's `app.properties` file and later override them on the deployment stage with Java system properties for a particular server instance.

### Configuring Middleware Servers

* Set up `cuba.webHostName`, `cuba.webPort`, `cuba.webContextName` application properties to form a unique [Server ID](https://doc.cuba-platform.com/manual-latest) for each middleware block.

* Set the following application properties for work in cluster:

        # Turn on cluster communication
        cuba.cluster.enabled = true
        
        # Use ZooKeeper for inter-server discovery
        cuba.cluster.jgroupsConfig = jgroups_zk.xml
        
        # Specify the address of the current server
        jgroups.bind_addr = 192.168.0.10
        
        # Specify ZooKeeper address
        jgroups.zkping.connection = 192.168.0.1:2181

### Configuring Web Client Blocks

* Set the following application properties to connect to the middleware cluster:

        # Use remote invocation
        cuba.useLocalServiceInvocation = false
        
        # Specify ZooKeeper address
        cubazk.connection = 192.168.0.1:2181
        
