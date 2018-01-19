/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.dbobject.index;

import com.neradb.common.DbException;
import com.neradb.engine.Session;
import com.neradb.result.Row;
import com.neradb.result.SearchRow;
import com.neradb.value.Value;
import com.neradb.value.ValueLong;

/**
 * The cursor implementation for the range index.
 */
class RangeCursor implements Cursor {

    private Session session;
    private boolean beforeFirst;
    private long current;
    private Row currentRow;
    private final long start, end, step;

    RangeCursor(Session session, long start, long end) {
        this(session, start, end, 1);
    }

    RangeCursor(Session session, long start, long end, long step) {
        this.session = session;
        this.start = start;
        this.end = end;
        this.step = step;
        beforeFirst = true;
    }

    @Override
    public Row get() {
        return currentRow;
    }

    @Override
    public SearchRow getSearchRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (beforeFirst) {
            beforeFirst = false;
            current = start;
        } else {
            current += step;
        }
        currentRow = session.createRow(new Value[]{ValueLong.get(current)}, 1);
        return step > 0 ? current <= end : current >= end;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

}
