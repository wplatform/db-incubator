/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.expression;

import java.util.ArrayList;

import com.neradb.api.ErrorCode;
import com.neradb.dbobject.Database;
import com.neradb.dbobject.table.Column;
import com.neradb.engine.Session;
import com.neradb.message.DbException;
import com.neradb.result.LocalResult;
import com.neradb.tools.SimpleResultSet;
import com.neradb.util.MathUtils;
import com.neradb.util.StatementBuilder;
import com.neradb.value.DataType;
import com.neradb.value.Value;
import com.neradb.value.ValueArray;
import com.neradb.value.ValueNull;
import com.neradb.value.ValueResultSet;

/**
 * Implementation of the functions TABLE(..) and TABLE_DISTINCT(..).
 */
public class TableFunction extends Function {
    private final boolean distinct;
    private final long rowCount;
    private Column[] columnList;

    TableFunction(Database database, FunctionInfo info, long rowCount) {
        super(database, info);
        distinct = info.type == Function.TABLE_DISTINCT;
        this.rowCount = rowCount;
    }

    @Override
    public Value getValue(Session session) {
        return getTable(session, args, false, distinct);
    }

    @Override
    protected void checkParameterCount(int len) {
        if (len < 1) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, getName(), ">0");
        }
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder(getName());
        buff.append('(');
        int i = 0;
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(columnList[i++].getCreateSQL()).append('=').append(e.getSQL());
        }
        return buff.append(')').toString();
    }


    @Override
    public String getName() {
        return distinct ? "TABLE_DISTINCT" : "TABLE";
    }

    @Override
    public ValueResultSet getValueForColumnList(Session session,
            Expression[] nullArgs) {
        return getTable(session, args, true, false);
    }

    public void setColumns(ArrayList<Column> columns) {
        this.columnList = new Column[columns.size()];
        columns.toArray(columnList);
    }

    private ValueResultSet getTable(Session session, Expression[] argList,
            boolean onlyColumnList, boolean distinctRows) {
        int len = columnList.length;
        Expression[] header = new Expression[len];
        Database db = session.getDatabase();
        for (int i = 0; i < len; i++) {
            Column c = columnList[i];
            ExpressionColumn col = new ExpressionColumn(db, c);
            header[i] = col;
        }
        LocalResult result = new LocalResult(session, header, len);
        if (distinctRows) {
            result.setDistinct();
        }
        if (!onlyColumnList) {
            Value[][] list = new Value[len][];
            int rows = 0;
            for (int i = 0; i < len; i++) {
                Value v = argList[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    list[i] = new Value[0];
                } else {
                    ValueArray array = (ValueArray) v.convertTo(Value.ARRAY);
                    Value[] l = array.getList();
                    list[i] = l;
                    rows = Math.max(rows, l.length);
                }
            }
            for (int row = 0; row < rows; row++) {
                Value[] r = new Value[len];
                for (int j = 0; j < len; j++) {
                    Value[] l = list[j];
                    Value v;
                    if (l.length <= row) {
                        v = ValueNull.INSTANCE;
                    } else {
                        Column c = columnList[j];
                        v = l[row];
                        v = c.convert(v);
                        v = v.convertPrecision(c.getPrecision(), false);
                        v = v.convertScale(true, c.getScale());
                    }
                    r[j] = v;
                }
                result.addRow(r);
            }
        }
        result.done();
        ValueResultSet vr = ValueResultSet.get(getSimpleResultSet(result,
                Integer.MAX_VALUE));
        return vr;
    }

    private static SimpleResultSet getSimpleResultSet(LocalResult rs,
            int maxrows) {
        int columnCount = rs.getVisibleColumnCount();
        SimpleResultSet simple = new SimpleResultSet();
        simple.setAutoClose(false);
        for (int i = 0; i < columnCount; i++) {
            String name = rs.getColumnName(i);
            int sqlType = DataType.convertTypeToSQLType(rs.getColumnType(i));
            int precision = MathUtils.convertLongToInt(rs.getColumnPrecision(i));
            int scale = rs.getColumnScale(i);
            simple.addColumn(name, sqlType, precision, scale);
        }
        rs.reset();
        for (int i = 0; i < maxrows && rs.next(); i++) {
            Object[] list = new Object[columnCount];
            for (int j = 0; j < columnCount; j++) {
                list[j] = rs.currentRow()[j].getObject();
            }
            simple.addRow(list);
        }
        return simple;
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        return getExpressionColumns(session,
                getTable(session, getArgs(), true, false).getResultSet());
    }

}
