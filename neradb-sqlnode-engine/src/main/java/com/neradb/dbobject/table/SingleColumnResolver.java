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
 * The single column resolver is like a table with exactly one row.
 * It is used to parse a simple one-column check constraint.
 */
public class SingleColumnResolver implements ColumnResolver {

    private final Column column;
    private Value value;

    SingleColumnResolver(Column column) {
        this.column = column;
    }

    @Override
    public String getTableAlias() {
        return null;
    }

    void setValue(Value value) {
        this.value = value;
    }

    @Override
    public Value getValue(Column col) {
        return value;
    }

    @Override
    public Column[] getColumns() {
        return new Column[] { column };
    }

    @Override
    public String getSchemaName() {
        return null;
    }

    @Override
    public TableFilter getTableFilter() {
        return null;
    }

    @Override
    public Select getSelect() {
        return null;
    }

    @Override
    public Column[] getSystemColumns() {
        return null;
    }

    @Override
    public Column getRowIdColumn() {
        return null;
    }

    @Override
    public Expression optimize(ExpressionColumn expressionColumn, Column col) {
        return expressionColumn;
    }

}
