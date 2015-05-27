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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.connector.cassandra.statements.SelectStatement;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.result.Result;

/**
 * Class CassandraQueryEngine: Allow to make select queries with the connector.
 */
public class CassandraQueryEngine implements IQueryEngine {
    private static final int DEFAULT_LIMIT = 10000;
    private int limit = DEFAULT_LIMIT;
    private Map<String, Session> sessions;
    Map<String,List<Pair<String,String>>> clusterProperties;

    /**
     * Basic constructor.
     *
     * @param sessions     Map of sessions.
     * @param properties Default limit for a query.
     */
    public CassandraQueryEngine(Map<String, Session> sessions, Map<String,List<Pair<String,String>>> properties) {
        this.sessions = sessions;
        this.clusterProperties=properties;
    }

    @Override
    public com.stratio.crossdata.common.result.QueryResult execute(String queryId,LogicalWorkflow workflow)
            throws ConnectorException {

        SelectStatement ss = new SelectStatement(workflow, limit, sessions, clusterProperties);
        String query = ss.parseQuery();

        Result result;
        if (ss.getSession() != null) {

            result = CassandraExecutor.execute(query, ss.getAliasColumns(), ss.getSession());

        } else {
            throw new ExecutionException("No session to cluster established");
        }
        return (QueryResult) result;

    }

    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow,
            IResultHandler resultHandler) throws ConnectorException {

        SelectStatement ss = new SelectStatement(workflow, limit, sessions, clusterProperties);
        String query = ss.parseQuery();

        if (ss.getSession() != null) {

            CassandraExecutor.asyncExecute(query, ss.getAliasColumns(), ss.getSession(), queryId, resultHandler);

        } else {
            throw new ExecutionException("No session to cluster established");
        }
    }

    @Override
    public void pagedExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler,
            int pageSize) throws ConnectorException {

        SelectStatement ss = new SelectStatement(workflow, limit, sessions, clusterProperties);
        String query = ss.parseQuery();

        if (ss.getSession() != null) {

            CassandraExecutor.asyncExecutePaging(query, ss.getAliasColumns(), ss.getSession(), queryId, resultHandler,
                    pageSize);

        } else {
            throw new ExecutionException("No session to cluster established");
        }

    }

    @Override
    public void stop(String queryId) throws ConnectorException {
        throw new UnsupportedException("Stop for Async execute not supported yet.");
    }

}
