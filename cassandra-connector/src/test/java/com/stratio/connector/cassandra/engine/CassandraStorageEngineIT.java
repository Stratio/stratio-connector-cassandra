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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandra;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class CassandraStorageEngineIT extends BasicCoreCassandra {

    private Map<String, Session> sessions;

    public int select2InsertTest(Session session, String query) {

        ResultSet res = session.execute(query);
        int numberOfRows = res.all().size();
        return numberOfRows;
    }



    @BeforeClass
    public void setUp() {
        BasicCoreCassandra.setUpBeforeClass();
        BasicCoreCassandra.loadTestData("cassandra_connector_demo", "demoKeyspace.cql");
        sessions = new HashMap<>();
        sessions.put("cluster", _session);
    }

    @Test
    public void basicInsert() {
        String query = "SELECT * FROM cassandra_connector_demo.users;";
        int initialCount = select2InsertTest(_session, query);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        TableName targetTable = new TableName("cassandra_connector_demo", "users");
        Map<Selector, Selector> options = new HashMap<>();
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
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

        //INSERT INTO cassandra_connector_demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phrase", new Cell("insert phase"));

        int finalCount = initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, row, false);
            finalCount = select2InsertTest(_session, query);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(initialCount, finalCount);
    }

    @Test
    public void InsertInExistingTableUnknownColumn() {
        String query = "SELECT * FROM cassandra_connector_demo.users;";
        int initialCount = select2InsertTest(_session, query);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        TableName targetTable = new TableName("cassandra_connector_demo", "users");
        Map<Selector, Selector> options = new HashMap<>();
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
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

        //INSERT INTO cassandra_connector_demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        //WRONG COLUMN
        row.addCell("prase", new Cell("insert phase"));

        int finalCount = initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, row, false);
            Assert.fail("Trying to add in a not existing column");
        }catch (ConnectorException e) {
            finalCount = select2InsertTest(_session, query);
        }
        Assert.assertEquals(initialCount, finalCount);
    }

    @Test
    public void multipleInsert() {
        String query = "SELECT * FROM cassandra_connector_demo.users;";
        int initialCount = select2InsertTest(_session, query);

        TableName targetTable = new TableName("cassandra_connector_demo", "users");
        Map<Selector, Selector> options = new HashMap<>();
        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        ClusterName clusterRef = new ClusterName("cluster");
        LinkedList<ColumnName> partitionKey = new LinkedList<>();
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

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        //INSERT INTO cassandra_connector_demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Collection<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.addCell("name", new Cell("insertName3"));
        row.addCell("gender", new Cell("male3"));
        row.addCell("email", new Cell("insertName3@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phrase", new Cell("insert phase3"));

        Row row2 = new Row();
        row2.addCell("name", new Cell("insertName2"));
        row2.addCell("gender", new Cell("female"));
        row2.addCell("email", new Cell("insertName2@doamin.com"));
        row2.addCell("age", new Cell(45));
        row2.addCell("bool", new Cell(true));
        row2.addCell("phrase", new Cell("insert phase2"));

        rows.add(row);
        rows.add(row2);

        int finalCount = initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, rows, false);
            finalCount = select2InsertTest(_session, query);
        } catch (ConnectorException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotEquals(initialCount, finalCount);
    }


    @Test
    public void deleteFromTest() {
        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);
        Collection<Filter> whereClauses=new HashSet<>();
        Selector leftSelector= new ColumnSelector(new ColumnName("cassandra_connector_demo","users","name"));
        Selector rightSelector= new StringSelector("insertName");
        Relation relation=new Relation(leftSelector, Operator.EQ, rightSelector);
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.DELETE_PK_EQ);
        Filter filter=new Filter(operationsSet,relation);

        Selector leftSelector2= new ColumnSelector(new ColumnName("cassandra_connector_demo","users","gender"));
        Selector rightSelector2= new StringSelector("male");
        Relation relation2=new Relation(leftSelector2, Operator.EQ, rightSelector2);
        Filter filter2=new Filter(operationsSet,relation2);
        whereClauses.add(filter);
        whereClauses.add(filter2);
        try {
            cse.delete(new ClusterName("cluster"), new TableName("cassandra_connector_demo", "users"), whereClauses);
            Assert.assertTrue(true);
        }catch (ConnectorException e){
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void truncateTest() {
        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        try {
            cse.truncate(new ClusterName("cluster"), new TableName("cassandra_connector_demo", "users_info"));
            Assert.assertTrue(true);
        }catch (ConnectorException e){
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void updateTableTest() {
        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);
        Collection<Filter> whereClauses=new HashSet<>();
        Selector leftSelector= new ColumnSelector(new ColumnName("cassandra_connector_demo","users","name"));
        Selector rightSelector= new StringSelector("name_2");
        Relation relation=new Relation(leftSelector, Operator.EQ, rightSelector);
        Set<Operations> operationsSet=new HashSet<>();
        operationsSet.add(Operations.DELETE_PK_EQ);
        Filter filter=new Filter(operationsSet,relation);

        Selector leftSelector2= new ColumnSelector(new ColumnName("cassandra_connector_demo","users","gender"));
        Selector rightSelector2= new StringSelector("male");
        Relation relation2=new Relation(leftSelector2, Operator.EQ, rightSelector2);
        Filter filter2=new Filter(operationsSet,relation2);
        whereClauses.add(filter);
        whereClauses.add(filter2);

        Selector leftSelector5= new ColumnSelector(new ColumnName("cassandra_connector_demo","users","email"));
        Selector rightSelector5= new StringSelector("name_2@domain.com");
        Relation relation5=new Relation(leftSelector5, Operator.EQ, rightSelector5);
        Filter filter5=new Filter(operationsSet,relation5);


        Selector leftSelector4= new ColumnSelector(new ColumnName("cassandra_connector_demo","users","age"));
        Selector rightSelector4= new IntegerSelector(12);
        Relation relation4=new Relation(leftSelector4, Operator.EQ, rightSelector4);
        Filter filter4=new Filter(operationsSet,relation4);
        whereClauses.add(filter);
        whereClauses.add(filter2);
        whereClauses.add(filter4);
        whereClauses.add(filter5);

        Collection<Relation> assignments=new HashSet<>();
        Selector leftSelector3=new ColumnSelector(new ColumnName("cassandra_connector_demo","users","phrase"));
        Selector rightSelector3= new StringSelector("Cambiando la frase....");
        Relation relation1=new Relation(leftSelector3, Operator.EQ, rightSelector3);
        assignments.add(relation1);

        try {
            cse.update(new ClusterName("cluster"), new TableName("cassandra_connector_demo", "users"), assignments, whereClauses);
            Assert.assertTrue(true);
        }catch (ConnectorException e){
            Assert.fail(e.getMessage());
        }

    }


    @AfterClass
    public void restore() {
        BasicCoreCassandra.dropKeyspaceIfExists("cassandra_connector_demo");
    }

}
