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

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.BasicCoreCassandraTest;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.terms.StringTerm;
import com.stratio.meta2.common.statements.structures.terms.Term;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class CassandraQueryEngineTest extends BasicCoreCassandraTest {

  @BeforeClass
  public static void setUpBeforeClass() {
    BasicCoreCassandraTest.setUpBeforeClass();
    BasicCoreCassandraTest.loadTestData("demo", "demoKeyspace.cql");
    //metadataManager = new MetadataManager(_session, null);
    //metadataManager.loadMetadata();
  }

  /*
  @Test
  public void basicSelect() {
    ClusterName targetCluster = new ClusterName("Cluster");

    List<LogicalStep> logicalSteps = new ArrayList<>();

    TableName tableName = new TableName("demo", "users");

    List<ColumnName> columnList = new ArrayList<>();
    ColumnName columnName = new ColumnName(tableName, "name");
    columnList.add(columnName);

    Project project = new Project(tableName, columnList);

    Selector identifier = new ColumnSelector(columnName);
    Term term = new StringTerm("name_5");
    List<Term<?>> terms = new ArrayList<>();
    terms.add(term);

    Relation relation = new Relation(identifier, Operator.LIKE, terms);
    Filter filter = new Filter(Operations.SELECT_LIMIT, relation);

    project.setNextStep(filter);

    logicalSteps.add(project);
    logicalSteps.add(filter);

    LogicalWorkflow workflow = new LogicalWorkflow(logicalSteps);

    Map<String, Session> sessions = new HashMap<>();
    sessions.put("Cluster", this._session);
    CassandraQueryEngine cqe = new CassandraQueryEngine(sessions);

    try {
      QueryResult qr = cqe.execute(targetCluster, workflow);
      String value = "";
      for (Row row : qr.getResultSet()) {
        Cell cell = row.getCell("name");
        value = cell.getValue().toString();
      }
      assertEquals(value, "name_5");
    } catch (UnsupportedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    assertEquals(cqe.parseQuery(), "SELECT name FROM demo.users WHERE name = 'name_5'");
  }
  */
}
