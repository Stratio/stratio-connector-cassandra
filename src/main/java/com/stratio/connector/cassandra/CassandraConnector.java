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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.engine.CassandraMetadataEngine;
import com.stratio.connector.cassandra.engine.CassandraQueryEngine;
import com.stratio.connector.cassandra.engine.CassandraStorageEngine;
import com.stratio.connector.cassandra.engine.Engine;
import com.stratio.connector.cassandra.engine.EngineConfig;
import com.stratio.crossdata.connectors.ConnectorApp;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.common.data.ClusterName;

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
    private String connectorName;
    private String[] datastoreName;
    private int defaultLimit;

    /**
     * Constructor.
     */
    public CassandraConnector() {
        sessions = new HashMap<>();
        try {
            InputStream inputStream = getClass()
                    .getResourceAsStream("/com/stratio/connector/cassandra/CassandraConnector.xml");
            Document d =
                    DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);

            //Search for the limit properties and connectorName
            XPathFactory xFactory = XPathFactory.newInstance();

            // create an XPath object
            XPath xpath = xFactory.newXPath();
            Object result;
            XPathExpression expr = null;
            try {
                expr = xpath.compile("//ConnectorName/text()");
                result = expr.evaluate(d, XPathConstants.NODESET);
                this.connectorName = ((NodeList) result).item(0).getNodeValue();
            } catch (XPathExpressionException e) {
                this.connectorName = "UNKNOWN";
            }

            try {
                expr = xpath.compile("//DataStores/DataStoreName/text()");
                result = expr.evaluate(d, XPathConstants.NODESET);
                datastoreName = new String[((NodeList) result).getLength()];
                for (int i = 0; i < ((NodeList) result).getLength(); i++) {
                    this.datastoreName[i] = ((NodeList) result).item(i).getNodeValue();

                }
            } catch (XPathExpressionException e) {
                datastoreName = new String[1];
                this.datastoreName[0] = "UNKNOWN";
            }

        } catch (SAXException e) {
            LOG.trace("Impossible to read Manifest with the connector configuration");
        } catch (IOException e) {
            LOG.trace("Impossible to read Manifest with the connector configuration");
        } catch (ParserConfigurationException e) {
            LOG.trace("Impossible to read Manifest with the connector configuration");
        }
    }

    /**
     * Main method that start the connector and controls the unexpected shutdowns
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
     * Get the name of the connector.
     *
     * @return The name.
     */
    @Override
    public String getConnectorName() {
        return connectorName;
    }

    /**
     * Get the name of the datastores required by the connector.
     *
     * @return The names.
     */
    @Override
    public String[] getDatastoreName() {
        return datastoreName.clone();
    }

    /**
     * Init Method with the needed configuration for the connector.
     * @param configuration
     * @throws InitializationException
     */
    @Override
    public void init(IConfiguration configuration) throws InitializationException {

    }

    /**
     * Connect Method: Enabled the connector with his own configuration
     * @param credentials: The credentials.
     * @param config: The cluster config
     * @throws ConnectionException
     */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config)
            throws ConnectionException {
        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getOptions();

        EngineConfig engineConfig = new EngineConfig();
        //the hosts must be received as [host1:port,host2:port,host3:port...]
        engineConfig.setCassandraHosts(
                clusterOptions.get("Hosts").substring(1, clusterOptions.get("Hosts").length() - 1)
                        .split(","));
        engineConfig.setCassandraPort(Integer.parseInt(clusterOptions.get("Port")));
        engineConfig.setCredentials(credentials);

        if (clusterOptions.get("DefaultLimit") == null) {
            defaultLimit = DEFAULT_LIMIT;
        } else {
            defaultLimit = Integer.parseInt(clusterOptions.get("DefaultLimit"));
        }

        Engine engine = new Engine(engineConfig);

        LOG.info("Cassandra session created.");

        sessions.put(clusterName.getName(), engine.getSession());
    }

    /**
     * Close the session of the cluster name specified.
     * @param name: Name of the cluster.
     * @throws ConnectionException
     */
    @Override
    public void close(ClusterName name) throws ConnectionException {
        sessions.get(name.getName()).close();
        sessions.remove(name.getName());
    }

    /**
     * Close all the sessions of the connector.
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
     * Close at the moment all the sessions of the connector
     */
    public void uncontrolledShutdown() {
        for (Session s : sessions.values()) {
            s.close();
        }
        sessions = new HashMap<>();
    }

    /**
     * Controls if there is a session started for a cluster name.
     * @param name: cluster name.
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
        return new CassandraStorageEngine(sessions);
    }

    /**
     * Get the query com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IQueryEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return new CassandraQueryEngine(sessions, defaultLimit);
    }

    /**
     * Get the metadata com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IMetadataEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return new CassandraMetadataEngine(sessions);
    }

}
