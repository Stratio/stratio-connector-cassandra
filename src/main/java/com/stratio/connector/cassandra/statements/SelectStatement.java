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
package com.stratio.connector.cassandra.statements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.OrderBy;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.logicalplan.TransformationStep;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;

public class SelectStatement {

    private static final int DEFAULT_LIMIT = 10000;
    private int limit = DEFAULT_LIMIT;
    private Map<Selector, String> aliasColumns = new HashMap<>();
    private List<OrderByClause> orderByColumns = new ArrayList<>();
    private List<FunctionSelector> functions = new ArrayList<>();
    private boolean catalogInc;
    private String catalog;
    private TableName tableName;
    private boolean whereInc = false;
    private boolean limitInc = true;
    private List<Relation> where = new ArrayList<>();
    private boolean luceneIndexExist = false;
    private StringBuilder luceneIndex = new StringBuilder();
    private Session session = null;


    public SelectStatement(LogicalWorkflow workflow, int limit, Map<String, Session> sessions){
        this.limit = limit;
        LogicalStep logicalStep = workflow.getInitialSteps().get(0);
        while (logicalStep != null) {
            if (logicalStep instanceof TransformationStep) {
                getTransformationStep(logicalStep,sessions);
            }
            logicalStep = logicalStep.getNextStep();
        }
    }


    private void getTransformationStep(LogicalStep logicalStep, Map<String, Session> sessions) {
        TransformationStep transformation = (TransformationStep) logicalStep;
        if (transformation instanceof Project) {
            Project project = (Project) transformation;
            session = sessions.get(project.getClusterName().getName());

            tableName = project.getTableName();

            catalogInc = tableName.isCompletedName();
            if (catalogInc) {
                CatalogName catalogName = tableName.getCatalogName();
                catalog = catalogName.getName();
            }
        } else {
            if (transformation instanceof Filter) {
                Filter filter = (Filter) transformation;
                whereInc = true;
                Relation relation = filter.getRelation();
                where.add(relation);
            } else if (transformation instanceof Limit) {
                Limit limitClause = (Limit) transformation;
                limit = limitClause.getLimit();
            } else if (transformation instanceof Select) {
                Select select = (Select) transformation;
                for (Selector selector : select.getColumnMap().keySet()) {
                    if (selector instanceof FunctionSelector) {
                        functions.add((FunctionSelector) selector);
                    }
                }
                aliasColumns = select.getColumnMap();
            } else if (transformation instanceof OrderBy) {
                OrderBy orderBy = (OrderBy) transformation;
                orderByColumns = orderBy.getIds();
            }
        }
    }


    /**
     * Method that convert a query to a cassandra language.
     *
     * @return java.lang.String with the Cassandra query.
     */
    public String parseQuery() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (aliasColumns != null && !aliasColumns.isEmpty()) {
            sb.append(getAliasClause());
        } else {
            sb.append(" * ");
        }
        sb.append(getFromClause());

        if (whereInc) {
            sb.append(getWhereClause());
        }

        if (!orderByColumns.isEmpty()) {
            sb.append(getOrderByClause());
        }

        if (limitInc) {
            sb.append(" LIMIT ").append(limit);
        }
        return sb.toString().replace("  ", " ");
    }

    private String getOrderByClause() {
        StringBuilder sb = new StringBuilder();
        sb.append(" ORDER BY ");
        int count = 0;
        for (OrderByClause orderByClause : orderByColumns) {
            if (count != 0) {
                sb.append(",");
            }
            count = 1;
            ColumnSelector columnSelector = (ColumnSelector) orderByClause.getSelector();
            sb.append(Utils.toCaseSensitive(columnSelector.getName().getName())).append(" ").append(orderByClause
                    .getDirection().name());
        }
        return sb.toString();
    }

    private String getWhereClause() {
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");

        String indexName=getLuceneIndex();
        if(!indexName.equals("")){
            luceneIndex.append(indexName).append("='{filter :  {type: \"boolean\", must:[");
        }

        int count = 0;
        for (Relation relation : where) {
            if (count > 0) {
                sb.append(" AND ");
            }
            count = 1;
            switch (relation.getOperator()) {
            case IN:
            case BETWEEN:
                break;
            case LIKE:
            case MATCH:
                if (luceneIndexExist) {
                    luceneIndex.append(",");
                }
                luceneIndex.append(getLuceneWhereClause(relation));
                luceneIndexExist = true;

                break;
            default:
                //Search if there is a range function that implies a new filter in lucene index.
                Selector right = relation.getRightTerm();
                if (right instanceof FunctionSelector) {
                    FunctionSelector function = (FunctionSelector) right;

                    if ("range".equalsIgnoreCase(function.getFunctionName())) {
                        getStringRangeFunction(function, (ColumnSelector) relation.getLeftTerm());
                    }else if("should".equalsIgnoreCase(function.getFunctionName())) {
                        getStringShouldFunction(function, relation);
                    }else {
                        ColumnSelector left = (ColumnSelector) relation.getLeftTerm();
                        String column = Utils.toCaseSensitive(left.getColumnName().getName());
                        sb.append(column).append(" ").append(relation.getOperator().toString()).append(" ");
                        sb.append(function.toString());
                    }

                } else {
                    ColumnSelector left = (ColumnSelector) relation.getLeftTerm();
                    String column = Utils.toCaseSensitive(left.getColumnName().getName());
                    sb.append(column).append(" ").append(relation.getOperator().toString()).append(" ");
                    sb.append(Utils.getFormatType(left, right, session));
                }

                break;
            }
        }

        if (luceneIndexExist) {
            StringBuilder sbLucene = new StringBuilder();
            sbLucene.append(luceneIndex).append(" ]}}'");
            sb.append(sbLucene);

        }

        String whereClause = sb.toString();

        while (whereClause.contains("WHERE  AND")) {
            whereClause = whereClause.replace("WHERE  AND", "WHERE");
        }
        return whereClause;

    }

    private void getStringShouldFunction(FunctionSelector function, Relation relation) {
        if (luceneIndexExist) {
            luceneIndex.append(",");
        }

        StringBuilder sbLucene = new StringBuilder();
        sbLucene.append(" {type: \"boolean\", should:[");
        Selector left=relation.getLeftTerm();
        String luceneOrIndex="";
        boolean first=true;
        for(Selector s:function.getFunctionColumns()) {
            if (!first){
                sbLucene.append(",");
            }
            Relation r=new Relation(left, Operator.MATCH, s);
            luceneOrIndex = getLuceneWhereClause(r);
            sbLucene.append(luceneOrIndex);
            first=false;
        }
        sbLucene.append(" ]}");
        luceneIndex.append(sbLucene);
        luceneIndexExist = true;
    }

    private void getStringRangeFunction(FunctionSelector function, ColumnSelector leftSelector) {

        if (luceneIndexExist) {
            luceneIndex.append(",");
        }
        String arg1 = function.getFunctionColumns().get(0).getStringValue();
        String arg2 = function.getFunctionColumns().get(1).getStringValue();
        String column = Utils.toCaseSensitive(leftSelector.getName().getName());
        luceneIndex.append("{type:\"range\", field:").append(column).append(", " +
                "lower:\"").append(arg1).append("\", upper:\"").append(arg2)
                .append("\",include_upper: true, include_lower: true }");

        luceneIndexExist = true;

    }

    private String getFromClause() {
        StringBuilder sb = new StringBuilder();
        sb.append(" FROM ");
        if (catalogInc) {
            sb.append(Utils.toCaseSensitive(catalog)).append(".");
        }
        sb.append(Utils.toCaseSensitive(tableName.getName()));
        return sb.toString();
    }

    private String getAliasClause() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<Selector, String> entry : aliasColumns.entrySet()) {
            if (i != 0) {
                sb.append(",");
            }
            i = 1;
            if (!(entry.getKey() instanceof FunctionSelector)) {
                sb.append(Utils.toCaseSensitive(entry.getKey().getColumnName().getName()));
            } else if (sb.length() == 0) {
                i = 0;

            }
        }

        for (FunctionSelector selectorFunction : functions) {
            String functionString = getFunctionString(selectorFunction);

            if (i != 0) {
                sb.append(",");
            }
            i = 1;
            sb.append(functionString);

        }
        return sb.toString();
    }

    private String getFunctionString(FunctionSelector selectorFunction) {
        String result;
        StringBuilder sb = new StringBuilder();
        switch (selectorFunction.getFunctionName().toUpperCase()) {
        case "COUNT":
            result = selectorFunction.getFunctionName() + "(*)";
            break;
        case "NOW":
            result = selectorFunction.getFunctionName() + "()";
            break;
        default:
            List<Selector> columns = selectorFunction.getFunctionColumns();
            sb.append(selectorFunction.getFunctionName()).append("(");
            for (Selector s : columns) {
                if (s instanceof ColumnSelector) {
                    ColumnSelector columnSelector = (ColumnSelector) s;
                    sb.append(Utils.toCaseSensitive(columnSelector.getColumnName().getName()));
                    sb.append(",");
                } else if (s instanceof FunctionSelector) {
                    FunctionSelector functionSelector = (FunctionSelector) s;
                    String subFunction = getFunctionString(functionSelector);
                    sb.append(subFunction);
                }
            }
            if (sb.toString().endsWith(",")) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }
            sb.append(")");
            result = sb.toString();
        }
        return result;
    }

    private String getLuceneIndex() {
        String indexName = "";
        List<ColumnMetadata> columns =
                session.getCluster().getMetadata().getKeyspace(Utils.toCaseSensitive(catalog)).getTable(
                        Utils.toCaseSensitive(tableName
                                .getName()))
                        .getColumns();
        for (ColumnMetadata column : columns) {
            if (column.getIndex() != null && column.getIndex().isCustomIndex()) {
                indexName = Utils.toCaseSensitive(column.getName());
            }
        }
        return indexName;
    }

    private String getLuceneWhereClause(Relation relation) {
        String result;

        StringBuilder sb = new StringBuilder();

        String column = relation.getLeftTerm().toString()
                .substring(relation.getLeftTerm().toString().lastIndexOf('.') + 1);

        String value = relation.getRightTerm().toString();
        // Generate query for column
        String[] processedQuery = processLuceneQueryType(value);
        sb.append("{type:\"");
        sb.append(processedQuery[0]);
        sb.append("\",field:\"");
        sb.append(column);
        sb.append("\",value:\"");
        sb.append(processedQuery[1]);
        sb.append("\"},");

        sb.replace(sb.length() - 1, sb.length(), "");

        result = sb.toString();

        return result;

    }

    /**
     * Process a query pattern to determine the type of Lucene query. The supported types of queries
     * are: <li>
     * <ul>
     * Wildcard: The query contains * or ?.
     * </ul>
     * <ul>
     * Fuzzy: The query ends with ~ and a number.
     * </ul>
     * <ul>
     * Regex: The query contains [ or ].
     * </ul>
     * <ul>
     * Match: Default query, supporting escaped symbols: *, ?, [, ], etc.
     * </ul>
     * </li>
     *
     * @param query The user query.
     * @return An array with the type of query and the processed query.
     */
    protected String[] processLuceneQueryType(String query) {
        String[] result = { "", "" };
        Pattern escaped = Pattern.compile(".*\\\\\\*.*|.*\\\\\\?.*|.*\\\\\\[.*|.*\\\\\\].*");
        Pattern wildcard = Pattern.compile(".*\\*.*|.*\\?.*");
        Pattern regex = Pattern.compile(".*\\].*|.*\\[.*");
        Pattern fuzzy = Pattern.compile(".*~\\d+");
        if (escaped.matcher(query).matches()) {
            result[0] = "match";
            result[1] =
                    query.replace("\\*", "*").replace("\\?", "?").replace("\\]", "]")
                            .replace("\\[", "[");
        } else if (regex.matcher(query).matches()) {
            result[0] = "regex";
            result[1] = query;
        } else if (fuzzy.matcher(query).matches()) {
            result[0] = "fuzzy";
            result[1] = query;
        } else if (wildcard.matcher(query).matches()) {
            result[0] = "wildcard";
            result[1] = query;
        } else {
            result[0] = "match";
            result[1] = query;
        }
        // C* Query builder doubles the ' character.
        result[1] = result[1].replaceAll("^'", "").replaceAll("'$", "");
        return result;
    }

    public Session getSession() {
        return session;
    }

    public Map<Selector, String> getAliasColumns() {
        return aliasColumns;
    }
}
