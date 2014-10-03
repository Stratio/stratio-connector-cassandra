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

import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;


public class CassandraConnectorTest extends BasicCoreCassandraTest {



    @Test
    public void connectorTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();
        if (cassandraConnector.getConnectorName() == null) {
            Assert.fail("Connector Name is empty");
        } else {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void connectorDataStoreTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        if (cassandraConnector.getDatastoreName() == null) {
            Assert.fail("DataStore Name is empty");
        } else {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void connectorConnectTest() {
        CassandraConnector cassandraConnector = new CassandraConnector();

        ICredentials credentials = null;
        Map<String, String> options = new HashMap<>();
        options.put("Hosts", "[127.0.0.1]");
        options.put("Port", "9042");

        ConnectorClusterConfig config =
            new ConnectorClusterConfig(new ClusterName("cluster"), options);

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
        Map<String, String> options = new HashMap<>();
        options.put("Hosts", "[127.0.0.1]");
        options.put("Port", "9042");

        ConnectorClusterConfig config =
            new ConnectorClusterConfig(new ClusterName("cluster"), options);
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
        Map<String, String> options = new HashMap<>();
        options.put("Hosts", "[127.0.0.1]");
        options.put("Port", "9042");

        ConnectorClusterConfig config =
            new ConnectorClusterConfig(new ClusterName("cluster"), options);

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
        Map<String, String> options = new HashMap<>();
        options.put("Hosts", "[127.0.0.1]");
        options.put("Port", "9042");

        ConnectorClusterConfig config =
            new ConnectorClusterConfig(new ClusterName("cluster"), options);

        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IQueryEngine queryEngine = cassandraConnector.getQueryEngine();
            if (queryEngine!=null) {
                Assert.assertTrue(true);
            }else{
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
        Map<String, String> options = new HashMap<>();
        options.put("Hosts", "[127.0.0.1]");
        options.put("Port", "9042");

        ConnectorClusterConfig config =
            new ConnectorClusterConfig(new ClusterName("cluster"), options);

        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IMetadataEngine metadataEngine = cassandraConnector.getMetadataEngine();
            if (metadataEngine!=null) {
                Assert.assertTrue(true);
            }else{
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
        Map<String, String> options = new HashMap<>();
        options.put("Hosts", "[127.0.0.1]");
        options.put("Port", "9042");

        ConnectorClusterConfig config =
            new ConnectorClusterConfig(new ClusterName("cluster"), options);

        try {
            cassandraConnector.connect(credentials, config);
        } catch (ConnectionException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IStorageEngine storageEngine = cassandraConnector.getStorageEngine();
            if (storageEngine!=null) {
                Assert.assertTrue(true);
            }else{
                Assert.fail("It can't be null");
            }
        } catch (UnsupportedException e) {
            Assert.fail(e.getMessage());
        }

    }



}
