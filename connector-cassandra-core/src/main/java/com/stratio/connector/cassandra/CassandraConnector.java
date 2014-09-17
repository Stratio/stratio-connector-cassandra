/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.cassandra;

import com.datastax.driver.core.Session;
import com.stratio.connector.ConnectorApp;
import com.stratio.connector.cassandra.engine.*;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class CassandraConnector implements IConnector {

    private Map<String, Session> sessions;
    private String connectorName;


    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(CassandraConnector.class);

    public static void main(String[] args) {

        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(new CassandraConnector());
    }


    public CassandraConnector(){
        try {
            InputStream inputStream=getClass().getResourceAsStream("/com/stratio/connector/cassandra/CassandraConnector.xml");
            Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
            this.connectorName=d.getElementsByTagName("ConnectorName").item(0).getTextContent();

        } catch (SAXException e) {
            LOG.trace("Impossible to read Manifest with connector configuration");
        } catch (IOException e) {
            LOG.trace("Impossible to read Manifest with connector configuration");
        } catch (ParserConfigurationException e) {
            LOG.trace("Impossible to read Manifest with connector configuration");
        }
    }

    @Override
    public String getConnectorName() {
        return connectorName;
    }

    /**
     * Get the name of the datastore required by the connector.
     *
     * @return The name.
     */
    @Override
    public String[] getDatastoreName() {
        return new String[] {"CassandraConnector"};
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {


    }

    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config)
        throws ConnectionException {
        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getOptions();

        EngineConfig engineConfig = new EngineConfig();
        //the hosts are received as [host1,host2,host3...]
        engineConfig.setCassandraHosts(clusterOptions.get("Hosts").substring(1,clusterOptions.get("Hosts").length()-1).split(","));
        engineConfig.setCassandraPort(Integer.parseInt(clusterOptions.get("Port")));
        engineConfig.setCredentials(credentials);

        Engine engine = new Engine(engineConfig);
        sessions.put(clusterName.getName(), engine.getSession());
    }

    @Override
    public void close(ClusterName name) throws ConnectionException {
        sessions.get(name.getName()).close();
        sessions.remove(name.getName());
    }

    @Override
    public boolean isConnected(ClusterName name) {
        boolean connected;
        /*
        if (sessions.get(name.getName()) != null) {
            connected = true;
        } else {
            connected = false;
        }
        */

        if (sessions.get(name.getName()) != null) {
            if (sessions.get(name.getName()).getCluster()!=null){
                connected=true;
            }else{
                connected=false;
            }
        } else {
            connected = false;
        }
        return connected;
    }

    /**
     * Get the storage com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.meta.common.connector.IStorageEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {
        IStorageEngine storageEngine = new CassandraStorageEngine(sessions);
        return storageEngine;
    }

    /**
     * Get the query com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.meta.common.connector.IQueryEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        IQueryEngine queryEngine = new CassandraQueryEngine(sessions);
        return queryEngine;
    }

    /**
     * Get the metadata com.stratio.connector.cassandra.
     *
     * @return An implementation of {@link com.stratio.meta.common.connector.IMetadataEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        IMetadataEngine metadataEngine = new CassandraMetadataEngine(sessions);
        return metadataEngine;
    }

}
