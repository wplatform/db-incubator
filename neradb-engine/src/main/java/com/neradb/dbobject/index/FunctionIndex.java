/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.dbobject.index;

import java.util.HashSet;

import com.neradb.common.DbException;
import com.neradb.dbobject.table.Column;
import com.neradb.dbobject.table.FunctionTable;
import com.neradb.dbobject.table.IndexColumn;
import com.neradb.dbobject.table.TableFilter;
import com.neradb.engine.Session;
import com.neradb.result.Row;
import com.neradb.result.SearchRow;
import com.neradb.result.SortOrder;

/**
 * An index for a function that returns a result set. This index can only scan
 * through all rows, search is not supported.
 */
public class FunctionIndex extends BaseIndex {

    private final FunctionTable functionTable;

    public FunctionIndex(FunctionTable functionTable, IndexColumn[] columns) {
        initBaseIndex(functionTable, 0, null, columns, IndexType.createNonUnique(true));
        this.functionTable = functionTable;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (functionTable.isBufferResultSetToLocalTemp()) {
            return new FunctionCursor(session, functionTable.getResult(session));
        }
        return new FunctionCursorResultSet(session,
                functionTable.getResultSet(session));
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        if (masks != null) {
            throw DbException.getUnsupportedException("ALIAS");
        }
        long expectedRows;
        if (functionTable.canGetRowCount()) {
            expectedRows = functionTable.getRowCountApproximation();
        } else {
            expectedRows = database.getSettings().estimatedFunctionTableRows;
        }
        return expectedRows * 10;
    }

    @Override
    public void remove(Session session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public long getRowCount(Session session) {
        return functionTable.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return functionTable.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public String getPlanSQL() {
        return "function";
    }

    @Override
    public boolean canScan() {
        return false;
    }

}
