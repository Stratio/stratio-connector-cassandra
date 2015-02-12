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

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.CriticalExecutionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.Selector;

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
                    .QueryResult.createQueryResult(utils.transformToMetaResultSet(resultSet, new HashMap<Selector, String>()),0,true);
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

}
