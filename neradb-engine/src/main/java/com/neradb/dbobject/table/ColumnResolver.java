/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.dbobject.table;

import com.neradb.command.dml.Select;
import com.neradb.command.expression.Expression;
import com.neradb.command.expression.ExpressionColumn;
import com.neradb.value.Value;

/**
 * A column resolver is list of column (for example, a table) that can map a
 * column name to an actual column.
 */
public interface ColumnResolver {

    /**
     * Get the table alias.
     *
     * @return the table alias
     */
    String getTableAlias();

    /**
     * Get the column list.
     *
     * @return the column list
     */
    Column[] getColumns();

    /**
     * Get the list of system columns, if any.
     *
     * @return the system columns or null
     */
    Column[] getSystemColumns();

    /**
     * Get the row id pseudo column, if there is one.
     *
     * @return the row id column or null
     */
    Column getRowIdColumn();

    /**
     * Get the schema name.
     *
     * @return the schema name
     */
    String getSchemaName();

    /**
     * Get the value for the given column.
     *
     * @param column the column
     * @return the value
     */
    Value getValue(Column column);

    /**
     * Get the table filter.
     *
     * @return the table filter
     */
    TableFilter getTableFilter();

    /**
     * Get the select statement.
     *
     * @return the select statement
     */
    Select getSelect();

    /**
     * Get the expression that represents this column.
     *
     * @param expressionColumn the expression column
     * @param column the column
     * @return the optimized expression
     */
    Expression optimize(ExpressionColumn expressionColumn, Column column);

}
