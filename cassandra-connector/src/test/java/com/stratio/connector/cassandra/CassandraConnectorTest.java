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

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.common.data.ClusterName;

public class CassandraConnectorTest extends BasicCoreCassandraTest {

    @Test
    public void connectorTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();
        if (cassandraConnector.getConnectorManifestPath() == null) {
            Assert.fail("Connector Manifest is empty");
        } else {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void connectorDataStoreTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        if (cassandraConnector.getDatastoreManifestPath() == null) {
            Assert.fail("DataStore Manifest is empty");
        } else {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void connectorConnectTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);

        try {
            cassandraConnector.connect(credentials, config);
            Assert.assertTrue(true);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void connectorClose() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);

        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
        try {
            cassandraConnector.close(new ClusterName("cluster"));
            Assert.assertTrue(true);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void connectorIsConnected() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);


        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        if (cassandraConnector.isConnected(new ClusterName("cluster"))) {
            Assert.assertTrue(true);
        } else {
            Assert.fail("Connector must be connected");
        }
    }

    @Test
    public void connectorQueryEngineTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);


        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IQueryEngine queryEngine = cassandraConnector.getQueryEngine();
            if (queryEngine != null) {
                Assert.assertTrue(true);
            } else {
                Assert.fail("It can't be null");
            }
        } catch (UnsupportedException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void connectorMetadataEngineTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);


        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IMetadataEngine metadataEngine = cassandraConnector.getMetadataEngine();
            if (metadataEngine != null) {
                Assert.assertTrue(true);
            } else {
                Assert.fail("It can't be null");
            }

        } catch (UnsupportedException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void connectorStorageEngineTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);


        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IStorageEngine storageEngine = cassandraConnector.getStorageEngine();
            if (storageEngine != null) {
                Assert.assertTrue(true);
            } else {
                Assert.fail("It can't be null");
            }
        } catch (UnsupportedException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void connectorStopTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);

        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
        try {
            cassandraConnector.shutdown();
            Assert.assertTrue(true);
        } catch (ExecutionException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void connectorUncontrolledStopTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> clusterOptions = new HashMap<>();
        clusterOptions.put("Hosts", "[127.0.0.1]");
        clusterOptions.put("Port", "9042");

        Map<String, String> connectorOptions = new HashMap<>();

        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName("cluster"), connectorOptions,
                clusterOptions);

        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }
        try {
            cassandraConnector.uncontrolledShutdown();
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


}
