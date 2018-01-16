/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.dbobject.index;

import com.neradb.dbobject.table.Table;
import com.neradb.message.DbException;
import com.neradb.result.ResultInterface;
import com.neradb.result.Row;
import com.neradb.result.SearchRow;
import com.neradb.value.Value;
import com.neradb.value.ValueNull;

/**
 * The cursor implementation of a view index.
 */
public class ViewCursor implements Cursor {

    private final Table table;
    private final ViewIndex index;
    private final ResultInterface result;
    private final SearchRow first, last;
    private Row current;

    public ViewCursor(ViewIndex index, ResultInterface result, SearchRow first,
            SearchRow last) {
        this.table = index.getTable();
        this.index = index;
        this.result = result;
        this.first = first;
        this.last = last;
    }

    @Override
    public Row get() {
        return current;
    }

    @Override
    public SearchRow getSearchRow() {
        return current;
    }

    @Override
    public boolean next() {
        while (true) {
            boolean res = result.next();
            if (!res) {
                if (index.isRecursive()) {
                    result.reset();
                } else {
                    result.close();
                }
                current = null;
                return false;
            }
            current = table.getTemplateRow();
            Value[] values = result.currentRow();
            for (int i = 0, len = current.getColumnCount(); i < len; i++) {
                Value v = i < values.length ? values[i] : ValueNull.INSTANCE;
                current.setValue(i, v);
            }
            int comp;
            if (first != null) {
                comp = index.compareRows(current, first);
                if (comp < 0) {
                    continue;
                }
            }
            if (last != null) {
                comp = index.compareRows(current, last);
                if (comp > 0) {
                    continue;
                }
            }
            return true;
        }
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

}
