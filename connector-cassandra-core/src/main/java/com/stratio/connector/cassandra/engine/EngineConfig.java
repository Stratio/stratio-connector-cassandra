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

package com.stratio.connector.cassandra.engine;

import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class EngineConfig {

  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(EngineConfig.class.getName());

  /**
   * Cassandra hosts.
   */
  private String [] cassandraHosts;

  /**
   * Cassandra port.
   */
  private int cassandraPort;


    /**
     * Cluster Name
     */

   private String clusterName;


  /**
   * Get Cassandra hosts.
   *
   * @return an array of hosts
   */
  public String[] getCassandraHosts() {
    return cassandraHosts.clone();
  }

  /**
   * Set cassandra hosts.
   *
   * @param cassandraHosts an array of String containing cassandra hosts.
   */
  public void setCassandraHosts(String[] cassandraHosts) {
    this.cassandraHosts = Arrays.copyOf(cassandraHosts, cassandraHosts.length);
  }

  /**
   * Get cassandra port.
   *
   * @return current cassandra port.
   */
  public int getCassandraPort() {
    return cassandraPort;
  }

  /**
   * Set cassandra port.
   *
   * @param cassandraPort Port of cassandra (CQL).
   */
  public void setCassandraPort(int cassandraPort) {
    this.cassandraPort = cassandraPort;
  }

    /**
      * Get cassandra cluster name.
      *
      * @return the cluster name.
      */
    public String getClusterName() {
        return clusterName;
    }

/**
 * Set cassandra port.
 *
 * @param clusterName .
 */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

  /**
   * Get cassandra host randomly.
   *
   * @return random cassandra host.
   */
  public String getRandomCassandraHost(){
    Random rand = new Random();
    return cassandraHosts[rand.nextInt(cassandraHosts.length)];
  }


}
