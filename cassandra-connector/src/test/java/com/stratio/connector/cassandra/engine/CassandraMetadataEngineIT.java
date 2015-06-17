/*
 * Licensed to STRATIO (C) under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership. The STRATIO
 * (C) licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.stratio.connector.cassandra.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandra;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import com.stratio.crossdata.common.utils.StringUtils;

public class CassandraMetadataEngineIT extends BasicCoreCassandra {

    private Map<String, Session> sessions;

    private int assertCatalog() {
        Session session = sessions.get("cluster");
        List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
        int numberOfKS = keyspaces.size();
        return numberOfKS;
    }

    private int assertTable(String catalog) {
        Session session = sessions.get("cluster");
        Collection<com.datastax.driver.core.TableMetadata> tables =
                _session.getCluster().getMetadata().getKeyspace(Utils.toCaseSensitive(catalog)).getTables();
        int numberOfTables = tables.size();
        return numberOfTables;
    }

    private String assertIndex(String columnIndex, String keySpace, String theTable) {
        String indexName = "";
        Session session = sessions.get("cluster");
        com.datastax.driver.core.TableMetadata table =
                session.getCluster().getMetadata().getKeyspace(Utils.toCaseSensitive(keySpace)).getTable(Utils
                        .toCaseSensitive(theTable));
        if (table.getColumn(Utils.toCaseSensitive(columnIndex)).getIndex() != null) {
            indexName = table.getColumn(Utils.toCaseSensitive(columnIndex)).getIndex().getName();
        }
        return indexName;
    }

    public void createTable(String tableName) {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        TableName targetTable = new TableName("demoMetadata", tableName);

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demoMetadata", tableName, "name");
        ColumnName partitionColumn2 = new ColumnName("demoMetadata", tableName, "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.BOOLEAN)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));


        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        try {
            cme.createTable(new ClusterName("cluster"), table);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void createTable(String catalog,String tableName) {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        TableName targetTable = new TableName(catalog, tableName);

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName(catalog, tableName, "name");
        ColumnName partitionColumn2 = new ColumnName(catalog, tableName, "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.BOOLEAN)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));


        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        try {
            cme.createTable(new ClusterName("cluster"), table);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void createCatalog() {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata = new CatalogMetadata(new CatalogName("demoMetadata"), options, tables);

        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void createCatalog(String name) {

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata = new CatalogMetadata(new CatalogName(name), options, tables);

        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void createIndex(String catalog, String table) {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName(catalog,table, "age"),
                new ColumnMetadata(new ColumnName(new TableName(catalog,table), "age"),
                        parameters, new ColumnType(DataType.TEXT)));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName(catalog,table, "Indice3"), columns,
                        IndexType.DEFAULT, null);
        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    @BeforeClass
    public void setUp() {
        BasicCoreCassandra.setUpBeforeClass();
        BasicCoreCassandra.loadTestData("cassandra_connector_demo", "demoKeyspace.cql");
        sessions = new HashMap<>();
        sessions.put("cluster", _session);
        createCatalog();
        createTable("demoMetadata","users1");
    }

    @Test
    public void createCatalogTest() {

        int rowsInitial = assertCatalog();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata4"), options, tables);
        int rowsFinal = rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            rowsFinal = assertCatalog();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

    }

    @Test
    public void createCatalogWithOptionsTest() {

        int rowsInitial = assertCatalog();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("REPLICATION"),
                new StringSelector("'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3"));
        options.put(new StringSelector("durable_writes"), new StringSelector("false"));

        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata5"), options, tables);
        int rowsFinal = rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            rowsFinal = assertCatalog();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

    }

    @Test
    public void alterCatalogWithOptionsTest() {

        boolean done=false;
        createCatalog("testAlterCatalog");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("REPLICATION"),
                new StringSelector("'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1"));
        options.put(new StringSelector("durable_writes"), new StringSelector("false"));

        try {
            cme.alterCatalog(new ClusterName("cluster"), new CatalogName("testAlterCatalog"), options);
            done=true;
            Assert.assertEquals(done,true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void createCatalogWithOptionsIncompleteTest() {

        int rowsInitial = assertCatalog();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("REPLICATION"),
                new StringSelector("'class' : 'SimpleStrategy'"));

        Map<TableName, TableMetadata> tables = new HashMap<>();

        CatalogMetadata catalogmetadata =
                new CatalogMetadata(new CatalogName("demoMetadata3"), options, tables);
        int rowsFinal = rowsInitial;
        try {
            cme.createCatalog(new ClusterName("cluster"), catalogmetadata);
            Assert.fail("Options incomplete");
        } catch (UnsupportedException e) {
            Assert.assertTrue(true);
        } catch (ExecutionException e) {
            Assert.assertTrue(true);
        }catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void createTableTest() {
        int rowsInitial = assertTable("demoMetadata");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demoMetadata", "users12");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demoMetadata", "users12", "name");
        ColumnName partitionColumn2 = new ColumnName("demoMetadata", "users12", "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.BOOLEAN)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));


        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable("demoMetadata");

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTableSinglePKTest() {
        int rowsInitial = assertTable("demoMetadata");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demoMetadata", "users11");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demoMetadata", "users11", "name");
        ColumnName clusterColumn1 = new ColumnName("demoMetadata", "users11", "bool");

        partitionKey.add(partitionColumn1);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        clusterKey.add(clusterColumn1);

        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.BOOLEAN)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));


        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable("demoMetadata");

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTablePKwihtClusterKeyTest() {
        int rowsInitial = assertTable("demoMetadata");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demoMetadata", "users10");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demoMetadata", "users10", "name");

        partitionKey.add(partitionColumn1);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.BOOLEAN)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable("demoMetadata");

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTableTestWithOptions() {
        int rowsInitial = assertTable("demoMetadata");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        StringSelector option1 = new StringSelector("insertion_time DESC");
        StringSelector option2 = new StringSelector("Yes");
        StringSelector option3 = new StringSelector("'Important biological records'");
        //options.put(new StringSelector("CLUSTERING ORDER BY"), option1);
        //options.put(new StringSelector("COMPACT STORAGE"), option2);
        //options.put(new StringSelector("comment"), option3);
        options.put(new StringSelector("compression"), new StringSelector(
                "'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 64"));

        TableName targetTable = new TableName("demoMetadata", "users13");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demoMetadata", "users13", "name");
        ColumnName partitionColumn2 = new ColumnName("demoMetadata", "users13", "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "name"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "gender"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "age"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "bool"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.BOOLEAN)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "phrase"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columns.put(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("cassandra_connector_demo", "users"), "email"), parameters,
                        new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));


        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable("demoMetadata");
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void dropTest() {
        createCatalog("demoMetadata12");
        createTable("demoMetadata12","users14");
        createIndex("demoMetadata12","users14");

        //drop Index
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada12", "users14", "age"),
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata12", "users14"), "age"),
                        parameters, new ColumnType(DataType.TEXT)));
        IndexMetadata index =
                new IndexMetadata(new IndexName("demoMetadata12", "users14", "Indice3"), columns,
                        IndexType.DEFAULT, null);
        try {
            cme.dropIndex(new ClusterName("cluster"), index);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(assertIndex("age", "demoMetadata12", "users14"), "Indice3");

        //drop table
        int rowsInitial = assertTable("demoMetadata12");
        cme = new CassandraMetadataEngine(sessions);
        int rowsFinal = rowsInitial;
        try {
            cme.dropTable(new ClusterName("cluster"), new TableName("demoMetadata12", "users14"));
            rowsFinal = assertTable("demoMetadata12");
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

        //drop catalog
        rowsInitial = assertCatalog();
        rowsFinal = rowsInitial;
        try {
            cme.dropCatalog(new ClusterName("cluster"), new CatalogName("demoMetadata12"));
            rowsFinal = assertCatalog();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createSimpleIndexTest() {
        createCatalog("demoMetadata10");
        createTable("demoMetadata10","users1");

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada10", "users1", "email"),
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata10", "users1"), "email"),
                        parameters, new ColumnType(DataType.TEXT)));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demoMetadata10", "users1", "Indice"), columns,
                        IndexType.DEFAULT, null);

        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(assertIndex("email", "demoMetadata10", "users1"), "Indice");
    }

    @Test
    public void createSimpleIndexWithoutNameTest() {
        createCatalog("demoMetadata11");
        createTable("demoMetadata11","users5");

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada11", "users5", "email"),
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata11", "users5"), "email"),
                        parameters, new ColumnType(DataType.TEXT)));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName(new TableName("demoMetadata11", "users5"), "email"), columns,
                        IndexType.DEFAULT, null);

        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(assertIndex("email", "demoMetadata11", "users5"), "email");
    }

    @Test
    public void createLuceneIndexTest() {
        createCatalog("demoMetadata9");
        createTable("demoMetadata9","users1");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada9", "users1", "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata9", "users1"), "phrase"),
                        parameters, new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));

        Map<Selector, Selector> options = new LinkedHashMap<>();

        List<ColumnMetadata> columnsIndex = new ArrayList<>();
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata9", "users1"), "name"),
                        parameters, new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata9", "users1"), "age"),
                        parameters, new com.stratio.crossdata.common.metadata.ColumnType(DataType.INT)));
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demoMetadata9", "users1"), "email"),
                        parameters, new com.stratio.crossdata.common.metadata.ColumnType(DataType.TEXT)));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demoMetadata9", "users1", "IndiceLucene"), columns,
                        IndexType.FULL_TEXT, options);
        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }


    }


    @Test
    public void alterTableAlterColumnIncompatibleTypeTest() {
        createCatalog("demoMetadata7");
        createTable("demoMetadata7","users7");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demoMetadata7", "users7"),
                "bool"), parameters, new ColumnType(DataType.TEXT));
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ALTER_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demoMetadata7", "users7"),alterOptions);
            Assert.fail("Incompatible type to alter column");
        } catch (ConnectorException e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void alterTableAlterColumnCompatibleTypeTest() {
        createCatalog("demoMetadata6");
        createTable("demoMetadata6","users8");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demoMetadata6", "users8"),
                "name"), parameters, new ColumnType(DataType.VARCHAR));
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ALTER_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demoMetadata6", "users8"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void alterTableAddColumnTest() {
        createCatalog("demoMetadata13");
        createTable("demoMetadata13","users9");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demoMetadata13", "users9"),
                "other"), parameters, new ColumnType(DataType.VARCHAR));
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ADD_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demoMetadata13", "users9"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void alterTableDropColumnTest() {
        createCatalog("demoMetadata8");
        createTable("demoMetadata8","users9");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demoMetadata8", "users9"),
                "bool"), parameters, new ColumnType(DataType.VARCHAR));
        AlterOptions alterOptions=new AlterOptions(AlterOperation.DROP_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demoMetadata8", "users9"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void alterTableAlterOptionsTest() {
        createCatalog("demoMetadata14");
        createTable("demoMetadata14","users10");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<Selector, Selector> properties=new HashMap<>();
        Selector a= new StringSelector("comment");
        Selector b= new StringSelector("The new comment");
        properties.put(a,b);
        ColumnMetadata columnMetadata=null;
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ALTER_OPTIONS,properties,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demoMetadata14", "users10"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void provideMetadataTest(){
        createCatalog("importCatalog1");
        createCatalog("importCatalog11");
        createCatalog("importCatalog111");
        createTable("importCatalog1","importTables");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        try{
            List<CatalogMetadata> catalogMetadataList=cme.provideMetadata(new ClusterName("cluster"));
            boolean findit1=false;
            boolean findit11=false;
            boolean findit111=false;
            for(CatalogMetadata catalogMetadata:catalogMetadataList){
                if ("importCatalog1".equals(catalogMetadata.getName().getName())){
                    findit1=true;
                }
                if ("importCatalog11".equals(catalogMetadata.getName().getName())){
                    findit11=true;
                }
                if ("importCatalog111".equals(catalogMetadata.getName().getName())){
                    findit111=true;
                }
            }
            Assert.assertTrue(findit1,"importCatalog1 not imported");
            Assert.assertTrue(findit11,"importCatalog11 not imported");
            Assert.assertTrue(findit111,"importCatalog111 not imported");
        }catch(ConnectorException e){
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void provideCatalogMetadataTest(){
        createCatalog("importCatalog2");
        createCatalog("importCatalog22");
        createCatalog("importCatalog222");
        createTable("importCatalog2","importTables");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        try{
            CatalogMetadata catalogMetadata=cme.provideCatalogMetadata(new ClusterName("cluster"),
                    new CatalogName("importCatalog22"));
            Assert.assertEquals(catalogMetadata.getName().getName(), "importCatalog22", "importCatalog22 not imported");

        }catch(ConnectorException e){
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void provideTableMetadataTest(){
        createCatalog("importCatalog3");
        createTable("importCatalog3","importTables");
        createTable("importCatalog3","importTables2");
        createTable("importCatalog3","importTables3");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        try{
            TableMetadata tableMetadata=cme.provideTableMetadata(new ClusterName("cluster"),
                    new TableName("importCatalog3", "importTables2"));
            Assert.assertEquals(tableMetadata.getName().getName(),"importTables2", "importTables2 not imported");
            Map<ColumnName,ColumnMetadata> columns=tableMetadata.getColumns();

            TableName tableName=new TableName("importCatalog3","importTables2");
            ColumnMetadata columnMetadata=columns.get(new ColumnName(tableName,"name"));
            Assert.assertEquals(columnMetadata.getName().getName(), "name", "Columns not imported well");
            Assert.assertEquals(columnMetadata.getColumnType().getDataType(), new ColumnType(DataType.TEXT).getDataType(),
                    "Columns Type not imported well");
        }catch(ConnectorException e){
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void getIndexLuceneTest(){

        Session session = sessions.get("cluster");
        String sqlIndex="select index_name,index_options from system.schema_columns where keyspace_name = " +
                "'nebula' and columnfamily_name ='main'";

        Utils utils = new Utils();
        ResultSet result=session.execute(sqlIndex);
        QueryResult queryResult=com.stratio.crossdata.common.result
                .QueryResult.createQueryResult(utils.transformToMetaResultSet(result, new HashMap<Selector,
                        String>()), 0, true);


        for (com.stratio.crossdata.common.data.Row row:queryResult.getResultSet().getRows()){
            Map<String,Cell> rowCell=row.getCells();
            if(!rowCell.get("index_options").getValue().equals("null")){
                Map<Selector,Selector> map= StringUtils.convertJsonToOptions(new TableName("catalogTest", "tableFecha"),
                        rowCell.get("index_options").getValue().toString());

                String schema=map.get(new StringSelector("schema")).getStringValue();
                String fieldsString=schema.substring(schema.indexOf("fields:{")+9);
                String[] fields=fieldsString.split(",");
                for(String field:fields){
                    String[] fieldParts=field.split(":");
                    String fieldName=fieldParts[0];
                    String fieldType=fieldParts[2].replace("}","");
                }
            }
        }
    }



    @AfterClass
    public void restore() {
        BasicCoreCassandra.dropKeyspaceIfExists("demoMetadata");
        for(int i=1; i<15; i++){
            BasicCoreCassandra.dropKeyspaceIfExists("demoMetadata" + i);
        }
        BasicCoreCassandra.dropKeyspaceIfExists("testAlterCatalog");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog1");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog11");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog111");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog2");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog22");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog222");
        BasicCoreCassandra.dropKeyspaceIfExists("importCatalog3");

    }

}
