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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.CriticalExecutionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * CassandraExecutor allows to interact with the Cassandra Datastax Driver and execute the queries.
 */
public final class CassandraExecutor {


    /**
     * The {@link com.stratio.connector.cassandra.utils.Utils}.
     */
    private static Utils utils = new Utils();

    /**
     * Private class constructor as all methods are static.
     */
    private CassandraExecutor() {
    }

    /**
     * Executes a query from a String.
     *
     * @param query   The query in a String.
     * @param session Cassandra datastax java driver session.
     * @return a {@link com.stratio.crossdata.common.result.Result}.
     */
    public static com.stratio.crossdata.common.result.Result execute(String query, Session session)
            throws ConnectorException {
        ResultSet resultSet;
        try {
            resultSet = session.execute(query);
            return com.stratio.crossdata.common.result
                    .QueryResult.createQueryResult(utils.transformToMetaResultSet(resultSet, new HashMap<Selector,
                            String>()),0,true);
        } catch (UnsupportedOperationException unSupportException) {
            throw new UnsupportedException(unSupportException.getMessage(),unSupportException);
        } catch (DriverException dex) {
            throw new CriticalExecutionException(dex.getMessage(),dex);
        } catch (Exception ex) {
            throw new ExecutionException(ex.getMessage(),ex);
        }
    }

    /**
     * Executes a query from a String and add the alias in the Result for Selects .
     *
     * @param query        The query in a String.
     * @param aliasColumns The Map with the alias
     * @param session      Cassandra datastax java driver session.
     * @return a {@link com.stratio.crossdata.common.result.Result}.
     */
    public static com.stratio.crossdata.common.result.Result execute(String query,
            Map<Selector, String> aliasColumns, Session session)
            throws ConnectorException {
        try {
            ResultSet resultSet = session.execute(query);
            return com.stratio.crossdata.common.result
                    .QueryResult
                    .createQueryResult(utils.transformToMetaResultSet(resultSet, aliasColumns),0,true);
        } catch (UnsupportedOperationException unSupportException) {
            throw new UnsupportedException(unSupportException.getMessage(),unSupportException);
        } catch (DriverException dex) {
            throw new CriticalExecutionException(dex.getMessage(),dex);
        } catch (Exception ex) {
            throw new ExecutionException(ex.getMessage(),ex);
        }
    }

    /**
     * Obtain the existing keyspaces in cassandra.
     * @param session The cassandra session.
     * @return A list of {@link com.stratio.crossdata.common.metadata.CatalogMetadata} .
     */
    public static List<CatalogMetadata> getKeyspaces(Session session,String cluster){
        List<CatalogMetadata> catalogMetadataList=new ArrayList<>();
        List<KeyspaceMetadata> keyspaceMetadataList=session.getCluster().getMetadata().getKeyspaces();

        for (KeyspaceMetadata keyspaceMetadata: keyspaceMetadataList){

            //Catalogs
            CatalogName name=new CatalogName(keyspaceMetadata.getName());

            Map<Selector, Selector> options=new HashMap<>();
            Map<String,String> replicationOptions=new HashMap<>();

            for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
                options.put(new StringSelector(entry.getKey()),new StringSelector(entry.getValue()));
            }

            //Tables
            Map<TableName, TableMetadata> tables=getTablesFromKeyspace(session,keyspaceMetadata,cluster);

            CatalogMetadata catalogMetadata=new CatalogMetadata(name,options,tables);
            catalogMetadataList.add(catalogMetadata);
        }

        return catalogMetadataList;
    }

    /**
     * Get the specified Catalog from the cassandra keyspace.
     * @param session The cassandra session.
     * @param catalogName The catalog name of the cassandra keyspace.
     * @return A {@link com.stratio.crossdata.common.metadata.CatalogMetadata} .
     */
    public static CatalogMetadata getKeyspacesByName(Session session, CatalogName catalogName, String cluster) {

        KeyspaceMetadata keyspace=session.getCluster().getMetadata().getKeyspace(catalogName.getName());
        CatalogName name=new CatalogName(keyspace.getName());


        Map<Selector, Selector> options=new HashMap<>();
        Map<String,String> replicationOptions=new HashMap<>();

        for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
            options.put(new StringSelector(entry.getKey()),new StringSelector(entry.getValue()));
        }

        Map<TableName, TableMetadata> tables=getTablesFromKeyspace(session,keyspace,cluster);
        CatalogMetadata catalogMetadata=new CatalogMetadata(name,options,tables);
        return catalogMetadata;
    }

    /**
     * Get the Crossdata TableMetadata from a tableName that is search into cassandra table metadata.
     * @param session The cassandra session.
     * @param tableName The table name to search.
     * @return A {@link com.stratio.crossdata.common.metadata.TableMetadata} .
     */
    public static TableMetadata getTablesByTableName(Session session, TableName tableName, String cluster) {
        com.datastax.driver.core.TableMetadata cassandraTableMetadata=session.getCluster().getMetadata().getKeyspace
                (tableName.getCatalogName().getName()).getTable(tableName.getName());

        return getXDTableMetadata(session,cassandraTableMetadata,cluster);
    }

    /**
     * Get the tables from a specified keyspace.
     * @param session The cassandra session.
     * @param keyspaceMetadata The keyspace metadata.
     * @return A map of tables.
     */
    private static Map<TableName, TableMetadata> getTablesFromKeyspace(Session session,KeyspaceMetadata
            keyspaceMetadata, String cluster) {
        Map<TableName, TableMetadata> tables=new HashMap<>();
        Collection<com.datastax.driver.core.TableMetadata> cassandraTables=keyspaceMetadata.getTables();
        for(com.datastax.driver.core.TableMetadata cassandraTable:cassandraTables) {
            TableName tableName=new TableName(keyspaceMetadata.getName(),cassandraTable.getName());
            TableMetadata tableMetadata=getXDTableMetadata(session,cassandraTable,cluster);
            tables.put(tableName,tableMetadata);
        }
        return tables;
    }

    /**
     * Get the crossdata table metadata from a cassandra table metadata
     * @param session The cassandra session.
     * @param cassandraTableMetadata The cassandra table metadata.
     * @return A {@link com.stratio.crossdata.common.metadata.TableMetadata} .
     */
    private static TableMetadata getXDTableMetadata(Session session,com.datastax.driver.core.TableMetadata
            cassandraTableMetadata, String cluster) {
        Map<IndexName, IndexMetadata> indexes=new HashMap<>();
        LinkedHashMap<ColumnName, ColumnMetadata> columns=new LinkedHashMap<>();
        List<com.datastax.driver.core.ColumnMetadata> cassandraColumns=cassandraTableMetadata.getColumns();

        for(com.datastax.driver.core.ColumnMetadata cassandraColumn:cassandraColumns){

            //Columns
            ColumnName columnName=new ColumnName(cassandraTableMetadata.getKeyspace().getName(),
                    cassandraTableMetadata.getName(), cassandraColumn.getName());
            ColumnType columnType=utils.getCrossdataColumn(cassandraColumn.getType());
            cassandraColumn.getType().asJavaClass();
            ColumnMetadata columnMetadata=new ColumnMetadata(columnName,null, columnType);
            columns.put(columnName,columnMetadata);

            //Indexes
            com.datastax.driver.core.ColumnMetadata.IndexMetadata cassandraIndex=cassandraColumn.getIndex();
            if (cassandraIndex!=null) {
                IndexName indexName = new IndexName(cassandraTableMetadata.getKeyspace().getName(),
                        cassandraTableMetadata.getName(), cassandraIndex.getName());
                Map<ColumnName, ColumnMetadata> columnIndex = new HashMap<>();

                columnIndex.put(columnName, columnMetadata);
                IndexMetadata indexMetadata = new IndexMetadata(indexName, columnIndex,
                        cassandraIndex.isCustomIndex() ? IndexType.CUSTOM : IndexType.DEFAULT, null);
                indexes.put(indexName,indexMetadata);
            }
        }

        ClusterName clusterRef=new ClusterName(cluster);

        List<ColumnName> partitionKey=new ArrayList<>();
        List<com.datastax.driver.core.ColumnMetadata> partitionColumns=cassandraTableMetadata.getPartitionKey();
        for(com.datastax.driver.core.ColumnMetadata cassandraPartition:partitionColumns){
            ColumnName columnName=new ColumnName(cassandraPartition.getTable().getKeyspace().getName(),
                    cassandraPartition.getTable().getName(),
                    cassandraPartition.getName());
            partitionKey.add(columnName);
        }

        List<ColumnName> clusterKey=new ArrayList<>();
        List<com.datastax.driver.core.ColumnMetadata> clusterColumns=cassandraTableMetadata.getClusteringColumns();
        for(com.datastax.driver.core.ColumnMetadata cassandraClusterKey:clusterColumns){
            ColumnName columnName=new ColumnName(cassandraClusterKey.getTable().getKeyspace().getName(),
                    cassandraClusterKey.getTable().getName(),
                    cassandraClusterKey.getName());
            clusterKey.add(columnName);
        }

        TableName tableName=new TableName(cassandraTableMetadata.getKeyspace().getName(),
                cassandraTableMetadata.getName());


        return new TableMetadata(tableName,null,columns,indexes,clusterRef, partitionKey,clusterKey);
    }




}
