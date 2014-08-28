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

package com.stratio.connector.cassandra.statements;



import java.util.*;

/**
 * Class that models a {@code CREATE TABLE} statement of the META language.
 */
public class CreateTableStatement {

  /**
   * The name of the target table.
   */
  private String tableName;

  /**
   * A map with the name of the columns in the table and the associated data type.
   */
  private Map<String, String> columnsWithType;

  /**
   * The list of columns that are part of the primary key.
   */
  private List<String> primaryKey;

  /**
   * The list of columns that are part of the clustering key.
   */
  private List<String> clusterKey;


  /**
   * The type of primary key. Accepted values are:
   * <ul>
   * <li>1: If the primary key contains a single column.</li>
   * <li>2: If the primary key is composed of several columns but it does not contain a clustering
   * key.</li>
   * <li>3: If both the primary key and clustering key are specified.</li>
   * </ul>
   */
  private int primaryKeyType;

  private static final int PRIMARY_SINGLE = 1;
  private static final int PRIMARY_COMPOSED = 2;
  private static final int PRIMARY_AND_CLUSTERING_SPECIFIED = 3;

  /**
   * Whether the table should be created only if not exists.
   */
  private boolean ifNotExists;

  /**
   * Whether the table will be created.
   */
  private boolean createTable = false;

  /**
   * The number of the column associated with the primary key. This value is only used if the type
   * of primary key is {@code 1}.
   */
  private int columnNumberPK;

  /**
   * Whether the table should be created with a set of properties.
   */
  private boolean withProperties = false;

    /**
     * Catalog
     */
    private String catalog;

    /**
     * Indicates if there is a catalog specified in the table name
     */
    private boolean catalogInc;



  /**
   * Class constructor.
   *
   * @param tableName The name of the table.
   * @param columns A map with the name of the columns in the table and the associated data type.
   * @param primaryKey The list of columns that are part of the primary key.
   * @param clusterKey The list of columns that are part of the clustering key.
   * @param primaryKeyType The type of primary key.
   * @param columnNumberPK The number of the column associated with the primary key. This value is
   *        only used if the type of primary key is {@code 1}.
   */
  public CreateTableStatement(String tableName, Map<String, String> columns,
                              List<String> primaryKey, List<String> clusterKey, int primaryKeyType, int columnNumberPK) {

    if (tableName.contains(".")) {
      String[] ksAndTablename = tableName.split("\\.");
      catalog = ksAndTablename[0];
      this.tableName = ksAndTablename[1];
      catalogInc = true;
    } else {
      this.tableName = tableName;
    }
    this.columnsWithType = columns;
    this.primaryKey = primaryKey;
    this.clusterKey = clusterKey;
    this.primaryKeyType = primaryKeyType;
    this.columnNumberPK = columnNumberPK;
  }

  public Map<String, String> getColumnsWithTypes() {
    return columnsWithType;
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * Set the keyspace specified in the create table statement.
   *
   * @param keyspace The name of the keyspace.
   */
  public void setKeyspace(String keyspace) {
    this.catalog = keyspace;
  }



  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public void setWithProperties(boolean withProperties) {
    this.withProperties = withProperties;
  }

  public String getSinglePKString() {
    StringBuilder sb = new StringBuilder(" (");
    Set<String> keySet = columnsWithType.keySet();
    int i = 0;
    for (Iterator<String> it = keySet.iterator(); it.hasNext();) {
      String key = it.next();
      String vp = columnsWithType.get(key);
      sb.append(key).append(" ").append(vp);
      if (i == columnNumberPK) {
        sb.append(" PRIMARY KEY");
      }
      i++;
      if (it.hasNext()) {
        sb.append(", ");
      } else {
        sb.append(")");
      }
    }
    return sb.toString();
  }

  public String getCompositePKString() {
    StringBuilder sb = new StringBuilder("PRIMARY KEY (");
    if (primaryKeyType == PRIMARY_AND_CLUSTERING_SPECIFIED) {
      sb.append("(");
    }

    Iterator<String> pks = primaryKey.iterator();
    while (pks.hasNext()) {
      sb.append(pks.next());
      if (pks.hasNext()) {
        sb.append(", ");
      }
    }

    if (primaryKeyType == PRIMARY_AND_CLUSTERING_SPECIFIED) {
      sb.append(")");
      for (String key : clusterKey) {
        sb.append(", ").append(key);
      }
    }

    sb.append("))");
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Create table ");
    if (ifNotExists) {
      sb.append("IF NOT EXISTS ");
    }

    if (catalogInc) {
      sb.append(catalog).append(".");
    }
    sb.append(tableName);

    if (primaryKeyType == PRIMARY_SINGLE) {
      sb.append(getSinglePKString());
    } else {
      Set<String> keySet = columnsWithType.keySet();
      sb.append(" (");
      for (String key : keySet) {
        String vp = columnsWithType.get(key);
        sb.append(key).append(" ").append(vp).append(", ");
      }
      sb.append(getCompositePKString());
    }

    /*if (withProperties) {
      sb.append(" WITH ").append(StringUtils.stringList(properties, " AND "));
    }*/
    return sb.toString();
  }



}
