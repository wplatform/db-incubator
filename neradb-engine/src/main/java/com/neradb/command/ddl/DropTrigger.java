/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.ddl;

import com.neradb.api.ErrorCode;
import com.neradb.command.CommandInterface;
import com.neradb.dbobject.Database;
import com.neradb.dbobject.Right;
import com.neradb.dbobject.schema.Schema;
import com.neradb.dbobject.schema.TriggerObject;
import com.neradb.dbobject.table.Table;
import com.neradb.engine.Session;
import com.neradb.message.DbException;

/**
 * This class represents the statement
 * DROP TRIGGER
 */
public class DropTrigger extends SchemaCommand {

    private String triggerName;
    private boolean ifExists;

    public DropTrigger(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        TriggerObject trigger = getSchema().findTrigger(triggerName);
        if (trigger == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TRIGGER_NOT_FOUND_1, triggerName);
            }
        } else {
            Table table = trigger.getTable();
            session.getUser().checkRight(table, Right.ALL);
            db.removeSchemaObject(session, trigger);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TRIGGER;
    }

}
