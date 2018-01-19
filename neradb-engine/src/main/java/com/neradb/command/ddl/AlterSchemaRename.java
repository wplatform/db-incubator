/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.ddl;

import java.util.ArrayList;

import com.neradb.command.CommandInterface;
import com.neradb.common.DbException;
import com.neradb.common.ErrorCode;
import com.neradb.dbobject.Database;
import com.neradb.dbobject.schema.Schema;
import com.neradb.dbobject.schema.SchemaObject;
import com.neradb.engine.Session;

/**
 * This class represents the statement
 * ALTER SCHEMA RENAME
 */
public class AlterSchemaRename extends DefineCommand {

    private Schema oldSchema;
    private String newSchemaName;

    public AlterSchemaRename(Session session) {
        super(session);
    }

    public void setOldSchema(Schema schema) {
        oldSchema = schema;
    }

    public void setNewName(String name) {
        newSchemaName = name;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        if (!oldSchema.canDrop()) {
            throw DbException.get(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1,
                    oldSchema.getName());
        }
        if (db.findSchema(newSchemaName) != null ||
                newSchemaName.equals(oldSchema.getName())) {
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1,
                    newSchemaName);
        }
        session.getUser().checkSchemaAdmin();
        db.renameDatabaseObject(session, oldSchema, newSchemaName);
        ArrayList<SchemaObject> all = db.getAllSchemaObjects();
        for (SchemaObject schemaObject : all) {
            db.updateMeta(session, schemaObject);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_SCHEMA_RENAME;
    }

}
