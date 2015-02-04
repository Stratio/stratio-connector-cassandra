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
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
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
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class CassandraMetadataEngineTest extends BasicCoreCassandraTest {

    private Map<String, Session> sessions;

    private int assertCatalog() {
        Session session = sessions.get("cluster");
        List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
        int numberOfKS = keyspaces.size();
        return numberOfKS;
    }

    private int assertTable() {
        Session session = sessions.get("cluster");
        Collection<com.datastax.driver.core.TableMetadata> tables =
                _session.getCluster().getMetadata().getKeyspace("demometadata").getTables();
        int numberOfTables = tables.size();
        return numberOfTables;
    }

    private String assertIndex(String columnIndex, String keySpace, String theTable) {
        String indexName = "";
        Session session = sessions.get("cluster");
        com.datastax.driver.core.TableMetadata table =
                session.getCluster().getMetadata().getKeyspace(keySpace).getTable(theTable);
        if (table.getColumn(columnIndex).getIndex() != null) {
            indexName = table.getColumn(columnIndex).getIndex().getName();
        }
        return indexName;
    }

    public void createTable(String tableName) {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();
        TableName targetTable = new TableName("demometadata", tableName);

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", tableName, "name");
        ColumnName partitionColumn2 = new ColumnName("demometadata", tableName, "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", tableName), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", tableName), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", tableName), "email"),
                        parameters, ColumnType.TEXT));

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
        columns.put(new ColumnName(new TableName(catalog, tableName), "name"),
                new ColumnMetadata(new ColumnName(new TableName(catalog, tableName), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName(catalog, tableName), "gender"),
                new ColumnMetadata(new ColumnName(new TableName(catalog, tableName), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName(catalog, tableName), "age"),
                new ColumnMetadata(new ColumnName(new TableName(catalog, tableName), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName(catalog, tableName), "bool"),
                new ColumnMetadata(new ColumnName(new TableName(catalog, tableName), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName(catalog, tableName), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName(catalog, tableName), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName(catalog, tableName), "email"),
                new ColumnMetadata(new ColumnName(new TableName(catalog, tableName), "email"),
                        parameters, ColumnType.TEXT));

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

    public void createIndex() {
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometadata", "users1", "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "age"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demometadata", "users1", "Indice2"), columns,
                        IndexType.DEFAULT, null);
        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    @BeforeClass
    public void setUp() {
        BasicCoreCassandraTest.setUpBeforeClass();
        BasicCoreCassandraTest.loadTestData("demo", "demoKeyspace.cql");
        sessions = new HashMap<>();
        sessions.put("cluster", _session);
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
                new CatalogMetadata(new CatalogName("demometadata5"), options, tables);
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
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demometadata", "users");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users", "name");
        ColumnName partitionColumn2 = new ColumnName("demometadata", "users", "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTableSinglePKTest() {
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demometadata", "users4");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users4", "name");
        ColumnName clusterColumn1 = new ColumnName("demometadata", "users4", "bool");

        partitionKey.add(partitionColumn1);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        clusterKey.add(clusterColumn1);

        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users4"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTablePKwihtClusterKeyTest() {
        int rowsInitial = assertTable();
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);

        Map<Selector, Selector> options = new HashMap<>();

        TableName targetTable = new TableName("demometadata", "users6");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users6", "name");

        partitionKey.add(partitionColumn1);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users6"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();

        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createTableTestWithOptions() {
        int rowsInitial = assertTable();
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

        TableName targetTable = new TableName("demometadata", "users3");

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
        ColumnName partitionColumn1 = new ColumnName("demometadata", "users3", "name");
        ColumnName partitionColumn2 = new ColumnName("demometadata", "users3", "gender");
        partitionKey.add(partitionColumn1);
        partitionKey.add(partitionColumn2);

        LinkedList<ColumnName> clusterKey = new LinkedList<>();
        Object[] parameters = { };
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "name"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "name"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "gender"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "gender"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "age"),
                        parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "bool"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "bool"),
                        parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "phrase"),
                        parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demometadata", "users3"), "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users3"), "email"),
                        parameters, ColumnType.TEXT));

        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        TableMetadata table =
                new TableMetadata(targetTable, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);

        int rowsFinal = rowsInitial;
        try {
            cme.createTable(new ClusterName("cluster"), table);
            rowsFinal = assertTable();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void dropTest() {
        createCatalog();
        createTable("users1");
        createIndex();

        //drop Index
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users1", "age"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "age"),
                        parameters, ColumnType.TEXT));
        IndexMetadata index =
                new IndexMetadata(new IndexName("demometadata", "users1", "Indice2"), columns,
                        IndexType.DEFAULT, null);
        try {
            cme.dropIndex(new ClusterName("cluster"), index);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(assertIndex("age", "demometadata", "users1"), "Indice2");

        //drop table
        int rowsInitial = assertTable();
        cme = new CassandraMetadataEngine(sessions);
        int rowsFinal = rowsInitial;
        try {
            cme.dropTable(new ClusterName("cluster"), new TableName("demometadata", "users1"));
            rowsFinal = assertTable();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);

        //drop catalog
        rowsInitial = assertCatalog();
        rowsFinal = rowsInitial;
        try {
            cme.dropCatalog(new ClusterName("cluster"), new CatalogName("demometadata"));
            rowsFinal = assertCatalog();
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(rowsInitial, rowsFinal);
    }

    @Test
    public void createSimpleIndexTest() {
        createCatalog();
        createTable("users1");

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users1", "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "email"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demometadata", "users1", "Indice"), columns,
                        IndexType.DEFAULT, null);

        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(assertIndex("email", "demometadata", "users1"), "indice");
    }

    @Test
    public void createSimpleIndexWithoutNameTest() {
        createCatalog();
        createTable("users5");

        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users5", "email"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users5"), "email"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName(new TableName("demometadata", "users5"), "email"), columns,
                        IndexType.DEFAULT, null);

        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(assertIndex("email", "demometadata", "users5"), "email");
    }

    @Test
    public void createLuceneIndexTest() {
        createCatalog();
        createTable("users1");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Object[] parameters = { };
        columns.put(new ColumnName("demometada", "users1", "phrase"),
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "phrase"),
                        parameters, ColumnType.TEXT));

        Map<Selector, Selector> options = new LinkedHashMap<>();

        List<ColumnMetadata> columnsIndex = new ArrayList<>();
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "name"),
                        parameters, ColumnType.TEXT));
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "age"),
                        parameters, ColumnType.INT));
        columnsIndex.add(
                new ColumnMetadata(new ColumnName(new TableName("demometadata", "users1"), "email"),
                        parameters, ColumnType.TEXT));

        IndexMetadata indexMetadata =
                new IndexMetadata(new IndexName("demometadata", "users1", "IndiceLucene"), columns,
                        IndexType.FULL_TEXT, options);
        try {
            cme.createIndex(new ClusterName("cluster"), indexMetadata);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(assertIndex("indicelucene", "demometadata", "users1"), "indicelucene");
    }


    @Test
    public void alterTableAlterColumnIncompatibleTypeTest() {
        createCatalog();
        createTable("users7");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demometadata", "users7"),
                "bool"), parameters, ColumnType.TEXT);
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ALTER_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demometadata", "users7"),alterOptions);
            Assert.fail("Incompatible type to alter column");
        } catch (ConnectorException e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void alterTableAlterColumnCompatibleTypeTest() {
        createCatalog();
        createTable("users8");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demometadata", "users8"),
                "name"), parameters, ColumnType.VARCHAR);
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ALTER_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demometadata", "users8"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void alterTableAddColumnTest() {
        createCatalog();
        createTable("users9");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demometadata", "users9"),
                "other"), parameters, ColumnType.VARCHAR);
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ADD_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demometadata", "users9"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void alterTableDropColumnTest() {
        createCatalog();
        createTable("users9");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Object[] parameters = { };
        ColumnMetadata columnMetadata=new ColumnMetadata(new ColumnName(new TableName("demometadata", "users9"),
                "bool"), parameters, ColumnType.VARCHAR);
        AlterOptions alterOptions=new AlterOptions(AlterOperation.DROP_COLUMN,null,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demometadata", "users9"),alterOptions);
            Assert.assertTrue(true);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void alterTableAlterOptionsTest() {
        createCatalog();
        createTable("users10");
        CassandraMetadataEngine cme = new CassandraMetadataEngine(sessions);
        Map<Selector, Selector> properties=new HashMap<>();
        Selector a= new StringSelector("comment");
        Selector b= new StringSelector("The new comment");
        properties.put(a,b);
        ColumnMetadata columnMetadata=null;
        AlterOptions alterOptions=new AlterOptions(AlterOperation.ALTER_OPTIONS,properties,columnMetadata);
        try {
            cme.alterTable(new ClusterName("cluster"),new TableName("demometadata", "users10"),alterOptions);
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
                if ("importcatalog1".equals(catalogMetadata.getName().getName())){
                    findit1=true;
                }
                if ("importcatalog11".equals(catalogMetadata.getName().getName())){
                    findit11=true;
                }
                if ("importcatalog111".equals(catalogMetadata.getName().getName())){
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
            Assert.assertEquals(catalogMetadata.getName().getName(), "importcatalog22", "importcatalog22 not imported");

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
            Assert.assertEquals(tableMetadata.getName().getName(),"importtables2", "importTables2 not imported");
            Map<ColumnName,ColumnMetadata> columns=tableMetadata.getColumns();

            TableName tableName=new TableName("importCatalog3","importTables2");
            ColumnMetadata columnMetadata=columns.get(new ColumnName(tableName,"name"));
            Assert.assertEquals(columnMetadata.getName().getName(), "name", "Columns not imported well");
            Assert.assertEquals(columnMetadata.getColumnType(), ColumnType.TEXT, "Columns Type not imported well");
        }catch(ConnectorException e){
            Assert.fail(e.getMessage());
        }
    }

    @AfterClass
    public void restore() {
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata");
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata2");
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata3");
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata4");
        BasicCoreCassandraTest.dropKeyspaceIfExists("demometadata5");
        BasicCoreCassandraTest.dropKeyspaceIfExists("testAlterCatalog");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog1");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog11");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog111");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog2");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog22");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog222");
        BasicCoreCassandraTest.dropKeyspaceIfExists("importCatalog3");

    }

}
