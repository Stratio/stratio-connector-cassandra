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

import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.CriticalExecutionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * CassandraExecutor allows to interact with the Cassandra Datastax Driver and execute the queries.
 */
public final class CassandraExecutor {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(CassandraExecutor.class);

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
        ResultSet resultSet = null;
        try {
            resultSet = session.execute(query);
            return com.stratio.crossdata.common.result
                    .QueryResult.createQueryResult(utils.transformToMetaResultSet(resultSet));
        } catch (UnsupportedOperationException unSupportException) {
            LOG.debug("Cassandra executor failed", unSupportException);
            throw new UnsupportedException(unSupportException);
        } catch (DriverException dex) {
            throw new CriticalExecutionException(dex);
        } catch (Exception ex) {
            throw new ExecutionException(ex);
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
            Map<ColumnName, String> aliasColumns, Session session)
            throws ConnectorException {
        try {
            ResultSet resultSet = session.execute(query);
            return com.stratio.crossdata.common.result
                    .QueryResult
                    .createQueryResult(utils.transformToMetaResultSet(resultSet, aliasColumns));
        } catch (UnsupportedOperationException unSupportException) {
            LOG.debug("Cassandra executor failed", unSupportException);
            throw new UnsupportedException(unSupportException);
        } catch (DriverException dex) {
            throw new CriticalExecutionException(dex);
        } catch (Exception ex) {
            throw new ExecutionException(ex);
        }
    }

}
