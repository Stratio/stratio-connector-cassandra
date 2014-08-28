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

package com.stratio.connector.cassandra.engine;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.connector.cassandra.statements.InsertIntoStatement;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.terms.GenericTerm;

import java.util.*;

/**
 * Created by jjlopez on 26/08/14.
 */
public class CassandraStorageEngine implements IStorageEngine {
    private Session session;

    public CassandraStorageEngine(Session session){
        this.session=session;
    }
    @Override
    public void insert(TableName targetTable, Row row) throws UnsupportedException, ExecutionException {
        String tableName=targetTable.getTableQualifiedName();

        Set<String> keys=row.getCells().keySet();
        List<String> columns=new ArrayList<String>();
        for (String key:keys){
            columns.add(key);
        }

        List<GenericTerm> cellValues=new ArrayList<GenericTerm>();
        for (String key:keys) {
            cellValues.add((GenericTerm)row.getCell(key).getValue());
        }

        InsertIntoStatement insertStatement=new InsertIntoStatement(tableName,columns,cellValues,true);
        CassandraExecutor.execute(insertStatement.toString(), session);
    }

    @Override
    public void insert(TableName targetTable, Collection<Row> rows) throws UnsupportedException, ExecutionException {
        String tableName=targetTable.getTableQualifiedName();

        for(Row row:rows){
            Set<String> keys=row.getCells().keySet();
            List<String> columns=new ArrayList<String>();
            for (String key:keys){
                columns.add(key);
            }

            List<GenericTerm> cellValues=new ArrayList<GenericTerm>();
            for (String key:keys) {
                cellValues.add((GenericTerm)row.getCell(key).getValue());
            }

            InsertIntoStatement insertStatement=new InsertIntoStatement(tableName,columns,cellValues,true);
            CassandraExecutor.execute(insertStatement.toString(), session);
        }

    }
}
