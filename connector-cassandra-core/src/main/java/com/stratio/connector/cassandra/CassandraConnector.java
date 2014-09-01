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
import com.stratio.connector.cassandra.engine.*;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.apache.log4j.Logger;

import java.util.Map;

public class CassandraConnector implements IConnector {

  private Map<String, Session> sessions;

  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(CassandraConnector.class);

  @Override
  public String getConnectorName() {
    return "Cassandra";
  }

  /**
   * Get the name of the datastore required by the connector.
   *
   * @return The name.
   */
  @Override
  public String[] getDatastoreName() {
    return new String[]{"Cassandra"};
  }

  @Override
  public void init(IConfiguration configuration) throws InitializationException {

  }

  @Override
  public void connect(ICredentials credentials, ConnectorClusterConfig config)
      throws ConnectionException {
    ClusterName clusterName = config.getName();
    Map<String, Object> options = config.getOptions();

    //getting the hosts and port of the cluster
    Map<String, Object> clusterOptions =
        (Map<String, Object>) options.get(clusterName.getName());

    EngineConfig engineConfig = new EngineConfig();
    engineConfig.setCassandraHosts((String[]) clusterOptions.get("Hosts"));
    engineConfig.setCassandraPort(Integer.parseInt((String) clusterOptions.get("Port")));

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
    if (sessions.get(name.getName()) != null) {
      connected = true;
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
