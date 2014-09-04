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


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Created by jjlopez on 29/08/14.
 */
public class CassandraStorageEngineTest extends BasicCoreCassandraTest {

    private Map<String, Session> sessions;


    public int select2InsertTest(Session session, String query) {

        ResultSet res = session.execute(query);
        int numberOfRows = res.all().size();
        return numberOfRows;
    }

    @BeforeClass
    public void setUp() {
        BasicCoreCassandraTest.setUpBeforeClass();
        BasicCoreCassandraTest.loadTestData("demo", "demoKeyspace.cql");
        sessions = new HashMap<>();
        sessions.put("cluster", _session);
    }

    @Test
    public void basicInsert() {
        String query = "SELECT * FROM demo.users;";
        int initialCount = select2InsertTest(_session, query);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);


        TableName targetTable=new TableName("demo", "users");
        Map<String, Object> options=new HashMap<>();
        Map< ColumnName, ColumnMetadata > columns=new HashMap<>();
        ClusterName clusterRef=new ClusterName("cluster");
        List<ColumnName> partitionKey=new ArrayList<>();
        List<ColumnName> clusterKey=new ArrayList<>();
        Object[] parameters=null;
        columns.put(new ColumnName(new TableName("demo","users"),"name"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"name"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo","users"),"gender"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"gender"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo","users"),"age"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"age"),parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demo","users"),"bool"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"bool"),parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demo","users"),"phrase"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"phrase"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo","users"),"email"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"email"),parameters, ColumnType.TEXT));


        Map<IndexName, IndexMetadata> indexes=new HashMap<>();
        TableMetadata table=new TableMetadata(targetTable,options,columns,indexes,clusterRef,partitionKey,clusterKey);

        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phrase", new Cell("insert phase"));

        int finalCount=initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, row);
            finalCount = select2InsertTest(_session, query);
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        Assert.assertNotEquals(initialCount,finalCount);
    }

    @Test
    public void multipleInsert() {
        String query = "SELECT * FROM demo.users;";
        int initialCount = select2InsertTest(_session, query);

        TableName targetTable=new TableName("demo", "users");
        Map<String, Object> options=new HashMap<>();
        Map< ColumnName, ColumnMetadata > columns=new HashMap<>();
        ClusterName clusterRef=new ClusterName("cluster");
        List<ColumnName> partitionKey=new ArrayList<>();
        List<ColumnName> clusterKey=new ArrayList<>();
        Object[] parameters=null;
        columns.put(new ColumnName(new TableName("demo","users"),"name"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"name"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo","users"),"gender"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"gender"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo","users"),"age"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"age"),parameters, ColumnType.INT));
        columns.put(new ColumnName(new TableName("demo","users"),"bool"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"bool"),parameters, ColumnType.BOOLEAN));
        columns.put(new ColumnName(new TableName("demo","users"),"phrase"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"phrase"),parameters, ColumnType.TEXT));
        columns.put(new ColumnName(new TableName("demo","users"),"email"),new ColumnMetadata(new ColumnName(new TableName("demo","users"),"email"),parameters, ColumnType.TEXT));


        Map<IndexName, IndexMetadata> indexes=new HashMap<>();
        TableMetadata table=new TableMetadata(targetTable,options,columns,indexes,clusterRef,partitionKey,clusterKey);

        CassandraStorageEngine cse = new CassandraStorageEngine(sessions);

        //INSERT INTO demo.users (name, gender, email, age, bool, phrase) VALUES ('name_0', 'male', 'name_0@domain.com', 10, true, '');
        Collection<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.addCell("name", new Cell("insertName"));
        row.addCell("gender", new Cell("male"));
        row.addCell("email", new Cell("insertName@doamin.com"));
        row.addCell("age", new Cell(20));
        row.addCell("bool", new Cell(false));
        row.addCell("phrase", new Cell("insert phase"));

        Row row2 = new Row();
        row2.addCell("name", new Cell("insertName2"));
        row2.addCell("gender", new Cell("female"));
        row2.addCell("email", new Cell("insertName2@doamin.com"));
        row2.addCell("age", new Cell(45));
        row2.addCell("bool", new Cell(true));
        row2.addCell("phrase", new Cell("insert phase2"));

        rows.add(row);
        rows.add(row2);

        int finalCount=initialCount;
        try {
            cse.insert(new ClusterName("cluster"), table, rows);
            finalCount = select2InsertTest(_session, query);
        } catch (UnsupportedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        Assert.assertNotEquals(initialCount,finalCount);
    }



}
