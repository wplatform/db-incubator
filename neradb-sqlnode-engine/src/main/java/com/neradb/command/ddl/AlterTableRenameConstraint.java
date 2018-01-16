/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.ddl;

import com.neradb.api.ErrorCode;
import com.neradb.command.CommandInterface;
import com.neradb.dbobject.Right;
import com.neradb.dbobject.constraint.Constraint;
import com.neradb.dbobject.schema.Schema;
import com.neradb.engine.Session;
import com.neradb.message.DbException;

/**
 * This class represents the statement
 * ALTER TABLE RENAME CONSTRAINT
 */
public class AlterTableRenameConstraint extends SchemaCommand {

    private String constraintName;
    private String newConstraintName;

    public AlterTableRenameConstraint(Session session, Schema schema) {
        super(session, schema);
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }
    public void setNewConstraintName(String newName) {
        this.newConstraintName = newName;
    }

    @Override
    public int update() {
        session.commit(true);
        Constraint constraint = getSchema().findConstraint(session, constraintName);
        if (constraint == null) {
            throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
        }
        if (getSchema().findConstraint(session, newConstraintName) != null ||
                newConstraintName.equals(constraintName)) {
            throw DbException.get(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1,
                    newConstraintName);
        }
        session.getUser().checkRight(constraint.getTable(), Right.ALL);
        session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
        session.getDatabase().renameSchemaObject(session, constraint, newConstraintName);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_RENAME_CONSTRAINT;
    }

}
