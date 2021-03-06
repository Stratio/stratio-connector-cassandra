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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.Cell;
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
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import com.stratio.crossdata.common.utils.StringUtils;

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
                            String>()), 0, true);
        } catch (UnsupportedOperationException unSupportException) {
            throw new UnsupportedException(unSupportException.getMessage(), unSupportException);
        } catch (DriverException dex) {
            throw new CriticalExecutionException(dex.getMessage(), dex);
        } catch (Exception ex) {
            throw new ExecutionException(ex.getMessage(), ex);
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
                    .createQueryResult(utils.transformToMetaResultSet(resultSet, aliasColumns), 0, true);
        } catch (UnsupportedOperationException unSupportException) {
            throw new UnsupportedException(unSupportException.getMessage(), unSupportException);
        } catch (DriverException dex) {
            throw new CriticalExecutionException(dex.getMessage(), dex);
        } catch (Exception ex) {
            throw new ExecutionException(ex.getMessage(), ex);
        }
    }

    /**
     * Executes an asynchronous query from a String and add the alias in the Result for Selects qith paging .
     *
     * @param query         The query in a String.
     * @param aliasColumns  The Map with the alias
     * @param session       Cassandra datastax java driver session.
     * @param queryId       The id of the query.
     * @param resultHandler The handler of the result.
     * @param pageSize      The number of fetching paging.
     */
    public static void asyncExecutePaging(String query,
            Map<Selector, String> aliasColumns, Session session, String queryId, IResultHandler resultHandler,
            int pageSize)
            throws ConnectorException {
        try {
            Statement st = new SimpleStatement(query);
            st.setFetchSize(pageSize);
            ResultSet resultSet = session.execute(st);
            int numPage = 0;
            List<ColumnDefinitions.Definition> definitions = resultSet.getColumnDefinitions().asList();

            List<Row> rows = new ArrayList<>();
            int i = 0;
            for (Row row : resultSet) {
                if (i < pageSize) {
                    rows.add(row);
                    i++;
                } else {
                    i = 0;
                    QueryResult queryResult = com.stratio.crossdata.common.result
                            .QueryResult
                            .createQueryResult(utils.transformPagingToMetaResultSet(
                                            definitions, rows, aliasColumns),
                                    numPage, false);
                    queryResult.setQueryId(queryId);
                    resultHandler.processResult(queryResult);
                    numPage++;
                    rows = new ArrayList<>();
                    rows.add(row);
                }
            }
            //Send the last result
            QueryResult queryResult = com.stratio.crossdata.common.result
                    .QueryResult
                    .createQueryResult(utils.transformPagingToMetaResultSet(
                                    definitions, rows, aliasColumns),
                            numPage, true);
            queryResult.setQueryId(queryId);
            resultHandler.processResult(queryResult);



        } catch (UnsupportedOperationException unSupportException) {
            resultHandler.processException(queryId, new ExecutionException(unSupportException.getMessage(),
                    unSupportException));
        } catch (DriverException dex) {
            resultHandler.processException(queryId, new ExecutionException(dex.getMessage()));
        } catch (Exception ex) {
            resultHandler.processException(queryId, new ExecutionException(ex.getMessage(), ex));
        }
    }

    /**
     * Executes an asynchronous query from a String and add the alias in the Result for Selects qith paging .
     *
     * @param query         The query in a String.
     * @param aliasColumns  The Map with the alias
     * @param session       Cassandra datastax java driver session.
     * @param queryId       The id of the query.
     * @param resultHandler The handler of the result.
     */
    public static void asyncExecute(String query,
            Map<Selector, String> aliasColumns, Session session, String queryId, IResultHandler resultHandler)
            throws ConnectorException {
        try {

            ResultSetFuture resultSet = session.executeAsync(query);

            while (!resultSet.isDone()) {

            }
            ResultSet partialResultSet = resultSet.get();

            QueryResult queryResult = com.stratio.crossdata.common.result
                    .QueryResult
                    .createQueryResult(utils.transformToMetaResultSet(partialResultSet, aliasColumns),
                            0, true);
            queryResult.setQueryId(queryId);
            resultHandler.processResult(queryResult);
        } catch (UnsupportedOperationException unSupportException) {
            resultHandler.processException(queryId, new ExecutionException(unSupportException.getMessage(),
                    unSupportException));
        } catch (DriverException dex) {
            resultHandler.processException(queryId, new ExecutionException(dex.getMessage()));
        } catch (Exception ex) {
            resultHandler.processException(queryId, new ExecutionException(ex.getMessage(), ex));
        }
    }

    /**
     * Obtain the existing keyspaces in cassandra.
     *
     * @param session The cassandra session.
     * @return A list of {@link com.stratio.crossdata.common.metadata.CatalogMetadata} .
     */
    public static List<CatalogMetadata> getKeyspaces(Session session, String cluster) {
        List<CatalogMetadata> catalogMetadataList = new ArrayList<>();
        List<KeyspaceMetadata> keyspaceMetadataList = session.getCluster().getMetadata().getKeyspaces();

        for (KeyspaceMetadata keyspaceMetadata : keyspaceMetadataList) {

            //Catalogs
            CatalogName name = new CatalogName(keyspaceMetadata.getName());

            Map<Selector, Selector> options = new HashMap<>();
            Map<String, String> replicationOptions = new HashMap<>();

            for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
                options.put(new StringSelector(entry.getKey()), new StringSelector(entry.getValue()));
            }

            //Tables
            Map<TableName, TableMetadata> tables = getTablesFromKeyspace(session, keyspaceMetadata, cluster);

            CatalogMetadata catalogMetadata = new CatalogMetadata(name, options, tables);
            catalogMetadataList.add(catalogMetadata);
        }

        return catalogMetadataList;
    }

    /**
     * Get the specified Catalog from the cassandra keyspace.
     *
     * @param session     The cassandra session.
     * @param catalogName The catalog name of the cassandra keyspace.
     * @return A {@link com.stratio.crossdata.common.metadata.CatalogMetadata} .
     */
    public static CatalogMetadata getKeyspacesByName(Session session, CatalogName catalogName, String cluster) {

        KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(Utils.toCaseSensitive(catalogName
                .getName()));
        CatalogName name = new CatalogName(keyspace.getName());

        Map<Selector, Selector> options = new HashMap<>();
        Map<String, String> replicationOptions = new HashMap<>();

        for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
            options.put(new StringSelector(entry.getKey()), new StringSelector(entry.getValue()));
        }

        Map<TableName, TableMetadata> tables = getTablesFromKeyspace(session, keyspace, cluster);
        CatalogMetadata catalogMetadata = new CatalogMetadata(name, options, tables);
        return catalogMetadata;
    }

    /**
     * Get the Crossdata TableMetadata from a tableName that is search into cassandra table metadata.
     *
     * @param session   The cassandra session.
     * @param tableName The table name to search.
     * @return A {@link com.stratio.crossdata.common.metadata.TableMetadata} .
     */
    public static TableMetadata getTablesByTableName(Session session, TableName tableName, String cluster)
            throws ConnectorException {
        Metadata clusterMetadata = session.getCluster().getMetadata();
        String ks = Utils.toCaseSensitive(tableName.getCatalogName().getName());

        KeyspaceMetadata ksMetadata = clusterMetadata.getKeyspace(ks);

        if(clusterMetadata.getKeyspace(ks) == null){
            throw new ConnectorException("Catalog " + ks + " not found");
        }

        String tn = Utils.toCaseSensitive(tableName.getName());
        com.datastax.driver.core.TableMetadata cassandraTableMetadata = ksMetadata.getTable(tn);

        if(ksMetadata.getTable(tn) == null){
            throw new ConnectorException("Table " + ks + "." + tn + " not found");
        }

        return getXDTableMetadata(session, cassandraTableMetadata, cluster);
    }

    /**
     * Get the tables from a specified keyspace.
     *
     * @param session          The cassandra session.
     * @param keyspaceMetadata The keyspace metadata.
     * @return A map of tables.
     */
    private static Map<TableName, TableMetadata> getTablesFromKeyspace(Session session, KeyspaceMetadata
            keyspaceMetadata, String cluster) {
        Map<TableName, TableMetadata> tables = new HashMap<>();
        Collection<com.datastax.driver.core.TableMetadata> cassandraTables = keyspaceMetadata.getTables();
        for (com.datastax.driver.core.TableMetadata cassandraTable : cassandraTables) {
            TableName tableName = new TableName(keyspaceMetadata.getName(), cassandraTable.getName());
            TableMetadata tableMetadata = getXDTableMetadata(session, cassandraTable, cluster);
            tables.put(tableName, tableMetadata);
        }
        return tables;
    }

    /**
     * Get the crossdata table metadata from a cassandra table metadata
     *
     * @param session                The cassandra session.
     * @param cassandraTableMetadata The cassandra table metadata.
     * @return A {@link com.stratio.crossdata.common.metadata.TableMetadata} .
     */
    private static TableMetadata getXDTableMetadata(Session session, com.datastax.driver.core.TableMetadata
            cassandraTableMetadata, String cluster) {
        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        List<com.datastax.driver.core.ColumnMetadata> cassandraColumns = cassandraTableMetadata.getColumns();

        for (com.datastax.driver.core.ColumnMetadata cassandraColumn : cassandraColumns) {

            //Columns
            ColumnName columnName = new ColumnName(cassandraTableMetadata.getKeyspace().getName(),
                    cassandraTableMetadata.getName(), cassandraColumn.getName());
            ColumnType columnType = utils.getCrossdataColumn(cassandraColumn.getType());
            ColumnMetadata columnMetadata = new ColumnMetadata(columnName, null, columnType);
            if (cassandraColumn.getIndex()==null || !cassandraColumn.getIndex().isCustomIndex()) {
                columns.put(columnName, columnMetadata);
            }

            //Indexes
            com.datastax.driver.core.ColumnMetadata.IndexMetadata cassandraIndex = cassandraColumn.getIndex();
            if (cassandraIndex != null) {
                IndexName indexName = new IndexName(cassandraTableMetadata.getKeyspace().getName(),
                        cassandraTableMetadata.getName(), cassandraIndex.getName());
                Map<ColumnName, ColumnMetadata> columnIndex=new HashMap<>();


                IndexMetadata indexMetadata;
                if (cassandraIndex.isCustomIndex()){
                    columnIndex=getLuceneIndex(session, indexName,cassandraIndex);
                    indexMetadata = new IndexMetadata(indexName, columnIndex, IndexType.FULL_TEXT , null);
                }else {
                    columnIndex.put(columnName, columnMetadata);
                    indexMetadata = new IndexMetadata(indexName, columnIndex, IndexType.DEFAULT, null);
                }
                indexes.put(indexName, indexMetadata);
            }
        }

        ClusterName clusterRef = new ClusterName(cluster);

        List<ColumnName> partitionKey = new ArrayList<>();
        List<com.datastax.driver.core.ColumnMetadata> partitionColumns = cassandraTableMetadata.getPartitionKey();
        for (com.datastax.driver.core.ColumnMetadata cassandraPartition : partitionColumns) {
            ColumnName columnName = new ColumnName(cassandraPartition.getTable().getKeyspace().getName(),
                    cassandraPartition.getTable().getName(),
                    cassandraPartition.getName());
            partitionKey.add(columnName);
        }

        List<ColumnName> clusterKey = new ArrayList<>();
        List<com.datastax.driver.core.ColumnMetadata> clusterColumns = cassandraTableMetadata.getClusteringColumns();
        for (com.datastax.driver.core.ColumnMetadata cassandraClusterKey : clusterColumns) {
            ColumnName columnName = new ColumnName(cassandraClusterKey.getTable().getKeyspace().getName(),
                    cassandraClusterKey.getTable().getName(),
                    cassandraClusterKey.getName());
            clusterKey.add(columnName);
        }

        TableName tableName = new TableName(cassandraTableMetadata.getKeyspace().getName(),
                cassandraTableMetadata.getName());

        return new TableMetadata(tableName, null, columns, indexes, clusterRef, partitionKey, clusterKey);
    }

    private static Map<ColumnName, ColumnMetadata> getLuceneIndex(Session session, IndexName indexName,
            com.datastax.driver.core.ColumnMetadata.IndexMetadata cassandraIndex) {

        Map<ColumnName, ColumnMetadata> columnMap=new HashMap<>();

        String table=cassandraIndex.getIndexedColumn().getTable().getName();
        String keyspace=cassandraIndex.getIndexedColumn().getTable().getKeyspace().getName();
        String sqlIndex="SELECT index_options from system.schema_columns where keyspace_name='"+ keyspace +
                "' and columnfamily_name ='" + table + "'";

        ResultSet result=session.execute(sqlIndex);
        QueryResult queryResult=com.stratio.crossdata.common.result
                .QueryResult.createQueryResult(utils.transformToMetaResultSet(result, new HashMap<Selector,
                String>()), 0, true);

        for (com.stratio.crossdata.common.data.Row row:queryResult.getResultSet().getRows()){
            Map<String,Cell> rowCell=row.getCells();
            if(!rowCell.get("index_options").getValue().equals("null")){
                Map<Selector,Selector> map=StringUtils.convertJsonToOptions(new TableName(keyspace,table),
                        rowCell.get("index_options").getValue().toString());

                if (!map.isEmpty()) {
                    String schema = map.get(new StringSelector("schema")).getStringValue();
                    String fieldsString = schema.substring(schema.indexOf("fields:{") + 8);
                    String[] fields = fieldsString.split(",");
                    for (String field : fields) {
                        String[] fieldParts = field.split(":");
                        if (fieldParts.length==3) {
                            String fieldName = fieldParts[0].replace("\"", "");
                            String fieldType = fieldParts[2].replace("}", "").replace("\"", "");
                            ColumnName columnName = new ColumnName(keyspace, table, fieldName);
                            ColumnType columnType = new ColumnType(Utils.getDataTypeFromString(fieldType));
                            columnType.setDbType(fieldType);
                            ColumnMetadata columnMetadata = new ColumnMetadata(columnName, null, columnType);
                            columnMap.put(columnName, columnMetadata);
                        }
                    }
                }
            }
        }
        return columnMap;
    }



}
