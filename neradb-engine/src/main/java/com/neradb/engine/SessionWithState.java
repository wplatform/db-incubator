/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.engine;

import java.util.ArrayList;

import com.neradb.command.CommandInterface;
import com.neradb.result.ResultInterface;
import com.neradb.util.New;
import com.neradb.value.Value;

/**
 * The base class for both remote and embedded sessions.
 */
abstract class SessionWithState implements SessionInterface {

    protected ArrayList<String> sessionState;
    protected boolean sessionStateChanged;
    private boolean sessionStateUpdating;

    /**
     * Re-create the session state using the stored sessionState list.
     */
    protected void recreateSessionState() {
        if (sessionState != null && sessionState.size() > 0) {
            sessionStateUpdating = true;
            try {
                for (String sql : sessionState) {
                    CommandInterface ci = prepareCommand(sql, Integer.MAX_VALUE);
                    ci.executeUpdate();
                }
            } finally {
                sessionStateUpdating = false;
                sessionStateChanged = false;
            }
        }
    }

    /**
     * Read the session state if necessary.
     */
    public void readSessionState() {
        if (!sessionStateChanged || sessionStateUpdating) {
            return;
        }
        sessionStateChanged = false;
        sessionState = New.arrayList();
        CommandInterface ci = prepareCommand(
                "SELECT * FROM INFORMATION_SCHEMA.SESSION_STATE",
                Integer.MAX_VALUE);
        ResultInterface result = ci.executeQuery(0, false);
        while (result.next()) {
            Value[] row = result.currentRow();
            sessionState.add(row[1].getString());
        }
    }

}
