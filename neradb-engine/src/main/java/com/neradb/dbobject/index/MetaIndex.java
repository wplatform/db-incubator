/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.dbobject.index;

import java.util.ArrayList;
import java.util.HashSet;

import com.neradb.common.DbException;
import com.neradb.dbobject.table.Column;
import com.neradb.dbobject.table.IndexColumn;
import com.neradb.dbobject.table.MetaTable;
import com.neradb.dbobject.table.TableFilter;
import com.neradb.engine.Session;
import com.neradb.result.Row;
import com.neradb.result.SearchRow;
import com.neradb.result.SortOrder;

/**
 * The index implementation for meta data tables.
 */
public class MetaIndex extends BaseIndex {

    private final MetaTable meta;
    private final boolean scan;

    public MetaIndex(MetaTable meta, IndexColumn[] columns, boolean scan) {
        initBaseIndex(meta, 0, null, columns, IndexType.createNonUnique(true));
        this.meta = meta;
        this.scan = scan;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ArrayList<Row> rows = meta.generateRows(session, first, last);
        return new MetaCursor(rows);
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        if (scan) {
            return 10 * MetaTable.ROW_COUNT_APPROXIMATION;
        }
        return getCostRangeIndex(masks, MetaTable.ROW_COUNT_APPROXIMATION,
                filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public void remove(Session session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public int getColumnIndex(Column col) {
        if (scan) {
            // the scan index cannot use any columns
            return -1;
        }
        return super.getColumnIndex(col);
    }

    @Override
    public boolean isFirstColumn(Column column) {
        if (scan) {
            return false;
        }
        return super.isFirstColumn(column);
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public long getRowCount(Session session) {
        return MetaTable.ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getRowCountApproximation() {
        return MetaTable.ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getDiskSpaceUsed() {
        return meta.getDiskSpaceUsed();
    }

    @Override
    public String getPlanSQL() {
        return "meta";
    }

}
