/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.engine.CassandraMetadataEngine;
import com.stratio.connector.cassandra.engine.CassandraQueryEngine;
import com.stratio.connector.cassandra.engine.CassandraStorageEngine;
import com.stratio.connector.cassandra.engine.Engine;
import com.stratio.connector.cassandra.engine.EngineConfig;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.connectors.ConnectorApp;

/**
 * Cassandra Connector class. This class contain a main that starts the connector.
 */
public class CassandraConnector implements IConnector {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(CassandraConnector.class);

    /**
     * DEFAULT_LIMIT for the select queries.
     */
    private static final int DEFAULT_LIMIT = 100;

    /**
     * The sessions of the connector.
     */
    private Map<String, Session> sessions;

    /**
     * Map of the clusterName with a list with the properties and values of the connector.
     */
    private Map<String, List<Pair<String, String>>> connectorOptionsPerCluster;


    /**
     * String  that contains the path to connector manifest.
     */
    private String connectorManifestPath;

    /**
     * String that contains the path to datastore manifest.
     */
    private String[] datastoreManifestPath=new String[1];

    /**
     * Engines
     */
    private CassandraStorageEngine storageEngine;
    private CassandraQueryEngine queryEngine;
    private CassandraMetadataEngine metadataEngine;

    /**
     * Constructor.
     */
    public CassandraConnector() {
        sessions = new HashMap<>();
        connectorOptionsPerCluster = new HashMap<>();
        connectorManifestPath=getClass().getResource("CassandraConnector.xml").getPath();
        datastoreManifestPath[0]=getClass().getResource("CassandraDataStore.xml").getPath();
    }

    /**
     * Main method that start the connector and controls the unexpected shutdowns.
     *
     * @param args
     */
    public static void main(String[] args) {

        CassandraConnector cassandraConnector = new CassandraConnector();

        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(cassandraConnector);
        cassandraConnector.attachShutDownHook();

    }

    private void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                uncontrolledShutdown();
            }
        });

    }

    /**
     * Init Method with the needed configuration for the connector.
     *
     * @param configuration
     * @throws InitializationException
     */
    @Override
    public void init(IConfiguration configuration) throws InitializationException {

    }

    /**
     * Connect Method: Enabled the connector with his own configuration.
     *
     * @param credentials The credentials.
     * @param config      The cluster config
     * @throws ConnectionException
     */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config)
            throws ConnectionException {
        ClusterName clusterName = config.getName();

        //Check if the cluster is attached with other connector
        if (sessions.containsKey(clusterName.getName())) {
            throw new ConnectionException("The connection to " + clusterName.getName() + " already exists.");
        }

        Map<String, String> clusterOptions = config.getClusterOptions();
        Map<String, String> connectorOptions = config.getConnectorOptions();

        EngineConfig engineConfig = new EngineConfig();
        //the hosts must be received as [host1:port,host2:port,host3:port...]
        engineConfig.setCassandraHosts(
                clusterOptions.get("Hosts").substring(1, clusterOptions.get("Hosts").length() - 1)
                        .split(","));
        engineConfig.setCassandraPort(Integer.parseInt(clusterOptions.get("Port")));
        engineConfig.setCredentials(credentials);

        Pair<String, String> connectorPropertiesValues;
        List<Pair<String, String>> connectorPropertiesList = new ArrayList<>();
        if (connectorOptions.get("DefaultLimit") == null) {
            connectorPropertiesValues = new ImmutablePair<>("DefaultLimit", Integer.toString(DEFAULT_LIMIT));
        } else {
            connectorPropertiesValues = new ImmutablePair<>("DefaultLimit", connectorOptions.get("DefaultLimit"));
        }
        connectorPropertiesList.add(connectorPropertiesValues);
        connectorOptionsPerCluster.put(clusterName.getName(), connectorPropertiesList);

        Engine engine = new Engine(engineConfig);

        LOG.info("Cassandra session created.");

        sessions.put(clusterName.getName(), engine.getSession());
    }

    /**
     * Close the session of the cluster name specified.
     *
     * @param name Name of the cluster.
     * @throws ConnectionException
     */
    @Override
    public void close(ClusterName name) throws ConnectionException {
        LOG.info("Closing cassandra session");
        sessions.get(name.getName()).close();
        sessions.remove(name.getName());
    }

    /**
     * Close all the sessions of the connector.
     *
     * @throws ExecutionException
     */
    @Override
    public void shutdown() throws ExecutionException {
        List<CloseFuture> closeFutureList = new ArrayList<>();
        for (Session s : sessions.values()) {
            closeFutureList.add(s.closeAsync());
        }
        sessions = new HashMap<>();
    }

    /**
     * Close at the moment all the sessions of the connector.
     */
    public void uncontrolledShutdown() {
        for (Session s : sessions.values()) {
            s.close();
        }
        sessions = new HashMap<>();
    }

    /**
     * Controls if there is a session started for a cluster name.
     *
     * @param name cluster name.
     * @return if the connector is connected to the cluster.
     */
    @Override
    public boolean isConnected(ClusterName name) {
        boolean connected;

        if (sessions.get(name.getName()) != null) {
            if (sessions.get(name.getName()).getCluster() != null) {
                connected = true;
            } else {
                connected = false;
            }
        } else {
            connected = false;
        }
        return connected;
    }

    /**
     * Get the storage com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IStorageEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {
        if (storageEngine != null) {
            return storageEngine;
        } else {
            return new CassandraStorageEngine(sessions);
        }
    }

    /**
     * Get the query com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IQueryEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        if (queryEngine != null) {
            return queryEngine;
        } else {
            return new CassandraQueryEngine(sessions, connectorOptionsPerCluster);
        }

    }

    /**
     * Get the metadata com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IMetadataEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        if (metadataEngine != null) {
            return metadataEngine;
        } else {
            return new CassandraMetadataEngine(sessions);
        }
    }

    @Override
    public String getConnectorManifestPath() {
        return connectorManifestPath;

    }

    @Override
    public String[] getDatastoreManifestPath() {
        return datastoreManifestPath;
    }

}
