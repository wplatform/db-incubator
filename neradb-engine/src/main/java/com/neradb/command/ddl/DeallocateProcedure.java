/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.ddl;

import com.neradb.command.CommandInterface;
import com.neradb.engine.Session;

/**
 * This class represents the statement
 * DEALLOCATE
 */
public class DeallocateProcedure extends DefineCommand {

    private String procedureName;

    public DeallocateProcedure(Session session) {
        super(session);
    }

    @Override
    public int update() {
        session.removeProcedure(procedureName);
        return 0;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DEALLOCATE;
    }

}
