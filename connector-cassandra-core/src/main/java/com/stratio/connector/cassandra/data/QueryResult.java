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

package com.stratio.connector.cassandra.data;



/**
 * Result of the execution of a query in META.
 */
public final class QueryResult extends com.stratio.meta.common.result.Result {

    /**
     * Serial version UID in order to be {@link java.io.Serializable}.
     */

    /**
     * Set of tuples returned by the server after successfully executing the query.
     */
    private final ResultSet resultSet;

    /**
     * Determine whether this query result contains the last resultset of the query.
     */
    private boolean lastResultSet = false;

    /**
     * Number of results sets that are part of the same query identifier sent to the client.
     */
    private int resultPage = 0;

    /**
     * The current user session catalog if it has been changed.
     */
    private String currentCatalog = null;

    /**
     * Whether the session catalog has changed.
     */
    private boolean catalogChanged = false;

    /**
     * Private class constructor of the factory.
     *
     * @param resultSet The set of tuples returned.
     */
    private QueryResult(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    /**
     * Get the set of results returned by the query.
     *
     * @return The set or null if an error occurred.
     */
    public ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * Set the new user session catalog.
     *
     * @param currentCatalog The new catalog.
     */
    public void setCurrentCatalog(String currentCatalog) {
        this.currentCatalog = currentCatalog;
        this.catalogChanged = true;
    }

    /**
     * Whether the session catalog has changed as an effect of executing the requested query.
     *
     * @return Whether it has changed or not.
     */
    public boolean isCatalogChanged() {
        return catalogChanged;
    }

    public String getCurrentCatalog() {
        return currentCatalog;
    }

    public void setLastResultSet() {
        this.lastResultSet = true;
    }

    public boolean isLastResultSet() {
        return lastResultSet;
    }

    public void setResultPage(int resultPage) {
        this.resultPage = resultPage;
    }

    public int getResultPage() {
        return resultPage;
    }

    public static QueryResult createSuccessQueryResult() {
        return new QueryResult(null);
    }

    /**
     * Create a successful query result.
     *
     * @param resultSet The associated {@link com.stratio.meta.common.data.ResultSet}
     * @return A {@link com.stratio.meta.common.result.QueryResult}.
     */
    public static QueryResult createQueryResult(ResultSet resultSet) {
        return new QueryResult(resultSet);
    }


    /**
     * Create a successful query result.
     *
     * @param resultSet The associated {@link com.stratio.meta.common.data.ResultSet}.
     * @param catalog   The new session catalog.
     * @return A {@link com.stratio.meta.common.result.QueryResult}.
     */
    public static QueryResult createSuccessQueryResult(ResultSet resultSet, String catalog) {
        QueryResult
            result = new QueryResult(resultSet);
        result.setCurrentCatalog(catalog);
        return result;
    }


}
