/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.jdbcx;

import java.util.StringTokenizer;

import javax.transaction.xa.Xid;

import com.neradb.common.DbException;
import com.neradb.common.ErrorCode;
import com.neradb.common.utils.StringUtils;
import com.neradb.message.TraceObject;

/**
 * An object of this class represents a transaction id.
 */
public class JdbcXid extends TraceObject implements Xid {

    private static final String PREFIX = "XID";

    private final int formatId;
    private final byte[] branchQualifier;
    private final byte[] globalTransactionId;

    JdbcXid(JdbcDataSourceFactory factory, int id, String tid) {
        setTrace(factory.getTrace(), TraceObject.XID, id);
        try {
            StringTokenizer tokenizer = new StringTokenizer(tid, "_");
            String prefix = tokenizer.nextToken();
            if (!PREFIX.equals(prefix)) {
                throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
            }
            formatId = Integer.parseInt(tokenizer.nextToken());
            branchQualifier = StringUtils.convertHexToBytes(tokenizer.nextToken());
            globalTransactionId = StringUtils.convertHexToBytes(tokenizer.nextToken());
        } catch (RuntimeException e) {
            throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
        }
    }

    /**
     * INTERNAL
     */
    public static String toString(Xid xid) {
        StringBuilder buff = new StringBuilder(PREFIX);
        buff.append('_').
            append(xid.getFormatId()).
            append('_').
            append(StringUtils.convertBytesToHex(xid.getBranchQualifier())).
            append('_').
            append(StringUtils.convertBytesToHex(xid.getGlobalTransactionId()));
        return buff.toString();
    }

    /**
     * Get the format id.
     *
     * @return the format id
     */
    @Override
    public int getFormatId() {
        debugCodeCall("getFormatId");
        return formatId;
    }

    /**
     * The transaction branch identifier.
     *
     * @return the identifier
     */
    @Override
    public byte[] getBranchQualifier() {
        debugCodeCall("getBranchQualifier");
        return branchQualifier;
    }

    /**
     * The global transaction identifier.
     *
     * @return the transaction id
     */
    @Override
    public byte[] getGlobalTransactionId() {
        debugCodeCall("getGlobalTransactionId");
        return globalTransactionId;
    }

}