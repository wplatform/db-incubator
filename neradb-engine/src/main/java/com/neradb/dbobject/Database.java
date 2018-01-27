package com.neradb.dbobject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.neradb.command.CommandInterface;
import com.neradb.command.ddl.CreateTableData;
import com.neradb.common.Constants;
import com.neradb.common.DbException;
import com.neradb.common.ErrorCode;
import com.neradb.common.SysProperties;
import com.neradb.common.io.FileUtils;
import com.neradb.common.utils.BitField;
import com.neradb.common.utils.CaseInsensitiveConcurrentMap;
import com.neradb.common.utils.CaseInsensitiveMap;
import com.neradb.common.utils.New;
import com.neradb.common.utils.NullableKeyConcurrentMap;
import com.neradb.common.utils.SmallLRUCache;
import com.neradb.common.utils.SourceCompiler;
import com.neradb.common.utils.StringUtils;
import com.neradb.common.utils.TempFileDeleter;
import com.neradb.common.utils.Utils;
import com.neradb.dbobject.constraint.Constraint;
import com.neradb.dbobject.index.Cursor;
import com.neradb.dbobject.index.Index;
import com.neradb.dbobject.index.IndexType;
import com.neradb.dbobject.schema.Schema;
import com.neradb.dbobject.schema.SchemaObject;
import com.neradb.dbobject.schema.TriggerObject;
import com.neradb.dbobject.table.Column;
import com.neradb.dbobject.table.IndexColumn;
import com.neradb.dbobject.table.MetaTable;
import com.neradb.dbobject.table.Table;
import com.neradb.dbobject.table.TableType;
import com.neradb.dbobject.table.TableView;
import com.neradb.engine.ConnectionInfo;
import com.neradb.engine.DatabaseCloser;
import com.neradb.engine.DbSettings;
import com.neradb.engine.MetaRecord;
import com.neradb.engine.Mode;
import com.neradb.engine.QueryStatisticsData;
import com.neradb.engine.Session;
import com.neradb.engine.UndoLogRecord;
import com.neradb.engine.spi.JavaObjectSerializer;
import com.neradb.engine.spi.TableEngine;
import com.neradb.message.Trace;
import com.neradb.message.TraceSystem;
import com.neradb.result.Row;
import com.neradb.result.RowFactory;
import com.neradb.result.SearchRow;
import com.neradb.store.DataHandler;
import com.neradb.store.FileStore;
import com.neradb.store.LobStorageFrontend;
import com.neradb.store.LobStorageInterface;
import com.neradb.store.LobStorageMap;
import com.neradb.util.CompareMode;
import com.neradb.util.JdbcUtils;
import com.neradb.value.Value;
import com.neradb.value.ValueInt;

public class Database implements DataHandler {

	private static final String SYSTEM_USER_NAME = "DBA";

	private final String databaseName;
	private final String cipher;
	private final byte[] filePasswordHash;
	private final byte[] fileEncryptionKey;

	private final HashMap<String, Role> roles = New.hashMap();
	private final HashMap<String, User> users = New.hashMap();
	private final HashMap<String, Setting> settings = New.hashMap();
	private final HashMap<String, Schema> schemas = New.hashMap();
	private final HashMap<String, Right> rights = New.hashMap();
	private final HashMap<String, UserDataType> userDataTypes = New.hashMap();
	private final HashMap<String, UserAggregate> aggregates = New.hashMap();
	private final HashMap<String, Comment> comments = New.hashMap();
	private final HashMap<String, TableEngine> tableEngines = New.hashMap();

	private final Set<Session> userSessions = Collections.synchronizedSet(new HashSet<Session>());
	private Session exclusiveSession;
	private final BitField objectIds = new BitField();
	private final Object lobSyncObject = new Object();

	private Schema mainSchema;
	private Schema infoSchema;
	private int nextSessionId;
	private int nextTempTableId;
	private User systemUser;
	private Session systemSession;
	private Session lobSession;
	private Table meta;
	private Index metaIdIndex;
	private boolean starting;
	private TraceSystem traceSystem;
	private Trace trace;
	private Role publicRole;
	private long modificationDataId;
	private long modificationMetaId;
	private CompareMode compareMode;
	private String cluster = Constants.CLUSTERING_DISABLED;
	private int maxMemoryRows = SysProperties.MAX_MEMORY_ROWS;
	private int maxMemoryUndo = Constants.DEFAULT_MAX_MEMORY_UNDO;
	private int lockMode = Constants.DEFAULT_LOCK_MODE;
	private int maxLengthInplaceLob;
	private int allowLiterals = Constants.ALLOW_LITERALS_ALL;

	private volatile boolean closing;
	private boolean ignoreCase;
	private String lobCompressionAlgorithm;
	private boolean optimizeReuseResults = true;
	private boolean referentialIntegrity = true;
	private boolean multiVersion;
	private DatabaseCloser closeOnExit;
	private Mode mode = Mode.getInstance(Mode.REGULAR);
	private boolean multiThreaded;
	private int maxOperationMemory = Constants.DEFAULT_MAX_OPERATION_MEMORY;
	private SmallLRUCache<String, String[]> lobFileListCache;
	private final TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
	private int compactMode;
	private SourceCompiler compiler;
	private volatile boolean metaTablesInitialized;
	private LobStorageInterface lobStorage;
	private int defaultTableType = Table.TYPE_CACHED;
	private final DbSettings dbSettings;
	private DbException backgroundException;
	private JavaObjectSerializer javaObjectSerializer;
	private String javaObjectSerializerName;
	private volatile boolean javaObjectSerializerInitialized;
	private boolean queryStatistics;
	private int queryStatisticsMaxEntries = Constants.QUERY_STATISTICS_MAX_ENTRIES;
	private QueryStatisticsData queryStatisticsData;
	private RowFactory rowFactory = RowFactory.DEFAULT;

	public Database(ConnectionInfo ci, String cipher) {
		String name = ci.getName();
		this.dbSettings = ci.getDbSettings();
		this.compareMode = CompareMode.getInstance(null, 0);
		this.filePasswordHash = new byte[] { 1 };
		this.fileEncryptionKey = new byte[] { 1 };
		this.databaseName = name;
		this.maxLengthInplaceLob = Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB;
		this.cipher = cipher;
		this.mode = Mode.getInstance("MySQL");
		this.multiVersion = true;
		this.multiThreaded = false;
		boolean closeAtVmShutdown = dbSettings.dbCloseOnExit;
		int traceLevelFile = TraceSystem.DEFAULT_TRACE_LEVEL_FILE;
		int traceLevelSystemOut = TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT;
		openDatabase(traceLevelFile, traceLevelSystemOut, closeAtVmShutdown);
	}

	private void openDatabase(int traceLevelFile, int traceLevelSystemOut, boolean closeAtVmShutdown) {
		try {
			open(traceLevelFile, traceLevelSystemOut);
			if (closeAtVmShutdown) {
				try {
					closeOnExit = new DatabaseCloser(this, 0, true);
					Runtime.getRuntime().addShutdownHook(closeOnExit);
				} catch (IllegalStateException e) {
					// shutdown in progress - just don't register the handler
					// (maybe an application wants to write something into a
					// database at shutdown time)
				} catch (SecurityException e) {
					// applets may not do that - ignore
					// Google App Engine doesn't allow
					// to instantiate classes that extend Thread
				}
			}
		} catch (Throwable e) {
			if (e instanceof OutOfMemoryError) {
				e.fillInStackTrace();
			}
			if (traceSystem != null) {
				if (e instanceof SQLException) {
					SQLException e2 = (SQLException) e;
					if (e2.getErrorCode() != ErrorCode.DATABASE_ALREADY_OPEN_1) {
						// only write if the database is not already in use
						trace.error(e, "opening {0}", databaseName);
					}
				}
				traceSystem.close();
			}
			closeOpenFilesAndUnlock(false);
			throw DbException.convert(e);
		}
	}

	/**
	 * Create a new row for a table.
	 *
	 * @param data
	 *            the values
	 * @param memory
	 *            whether the row is in memory
	 * @return the created row
	 */
	public Row createRow(Value[] data, int memory) {
		return rowFactory.createRow(data, memory);
	}

	public RowFactory getRowFactory() {
		return rowFactory;
	}

	public void setRowFactory(RowFactory rowFactory) {
		this.rowFactory = rowFactory;
	}

	/**
	 * Check if two values are equal with the current comparison mode.
	 *
	 * @param a
	 *            the first value
	 * @param b
	 *            the second value
	 * @return true if both objects are equal
	 */
	public boolean areEqual(Value a, Value b) {
		// can not use equals because ValueDecimal 0.0 is not equal to 0.00.
		return a.compareTo(b, compareMode) == 0;
	}

	/**
	 * Compare two values with the current comparison mode. The values may not be of
	 * the same type.
	 *
	 * @param a
	 *            the first value
	 * @param b
	 *            the second value
	 * @return 0 if both values are equal, -1 if the first value is smaller, and 1
	 *         otherwise
	 */
	public int compare(Value a, Value b) {
		return a.compareTo(b, compareMode);
	}

	/**
	 * Compare two values with the current comparison mode. The values must be of
	 * the same type.
	 *
	 * @param a
	 *            the first value
	 * @param b
	 *            the second value
	 * @return 0 if both values are equal, -1 if the first value is smaller, and 1
	 *         otherwise
	 */
	public int compareTypeSafe(Value a, Value b) {
		return a.compareTypeSafe(b, compareMode);
	}

	public long getModificationDataId() {
		return modificationDataId;
	}

	public long getNextModificationDataId() {
		return ++modificationDataId;
	}

	public long getModificationMetaId() {
		return modificationMetaId;
	}

	public long getNextModificationMetaId() {
		// if the meta data has been modified, the data is modified as well
		// (because MetaTable returns modificationDataId)
		modificationDataId++;
		return modificationMetaId++;
	}

	@Override
	public void checkPowerOff() {

	}

	/**
	 * Check if a database with the given name exists.
	 *
	 * @param name
	 *            the name of the database (including path)
	 * @return true if one exists
	 */
	static boolean exists(String name) {
		if (FileUtils.exists(name + Constants.SUFFIX_PAGE_FILE)) {
			return true;
		}
		return FileUtils.exists(name + Constants.SUFFIX_MV_FILE);
	}

	/**
	 * Get the trace object for the given module id.
	 *
	 * @param moduleId
	 *            the module id
	 * @return the trace object
	 */
	public Trace getTrace(int moduleId) {
		return traceSystem.getTrace(moduleId);
	}

	@Override
	public FileStore openFile(String name, String openMode, boolean mustExist) {
		if (mustExist && !FileUtils.exists(name)) {
			throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
		}
		FileStore store = FileStore.open(this, name, openMode, cipher, filePasswordHash);
		try {
			store.init();
		} catch (DbException e) {
			store.closeSilently();
			throw e;
		}
		return store;
	}

	/**
	 * Check if the file password hash is correct.
	 *
	 * @param testCipher
	 *            the cipher algorithm
	 * @param testHash
	 *            the hash code
	 * @return true if the cipher algorithm and the password match
	 */
	public boolean validateFilePasswordHash(String testCipher, byte[] testHash) {
		if (!StringUtils.equals(testCipher, this.cipher)) {
			return false;
		}
		return Utils.compareSecure(testHash, filePasswordHash);
	}

	private synchronized void open(int traceLevelFile, int traceLevelSystemOut) {

		String dataFileName = databaseName + Constants.SUFFIX_OLD_DATABASE_FILE;
		boolean existsData = FileUtils.exists(dataFileName);
		String pageFileName = databaseName + Constants.SUFFIX_PAGE_FILE;
		String mvFileName = databaseName + Constants.SUFFIX_MV_FILE;
		boolean existsPage = FileUtils.exists(pageFileName);
		boolean existsMv = FileUtils.exists(mvFileName);
		if (existsData && (!existsPage && !existsMv)) {
			throw DbException.get(ErrorCode.FILE_VERSION_ERROR_1, "Old database: " + dataFileName
					+ " - please convert the database " + "to a SQL script and re-create it.");
		}

		traceSystem = new TraceSystem(databaseName + Constants.SUFFIX_TRACE_FILE);
		traceSystem.setLevelFile(traceLevelFile);
		traceSystem.setLevelSystemOut(traceLevelSystemOut);
		trace = traceSystem.getTrace(Trace.DATABASE);
		trace.info("opening {0} (build {1})", databaseName, Constants.BUILD_ID);
 
		deleteOldTempFiles();

		starting = false;

		systemUser = new User(this, 0, SYSTEM_USER_NAME, true);
		mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
		infoSchema = new Schema(this, -1, "INFORMATION_SCHEMA", systemUser, true);
		schemas.put(mainSchema.getName(), mainSchema);
		schemas.put(infoSchema.getName(), infoSchema);
		publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
		roles.put(Constants.PUBLIC_ROLE_NAME, publicRole);
		systemUser.setAdmin(true);
		systemSession = new Session(this, systemUser, ++nextSessionId);
		lobSession = new Session(this, systemUser, ++nextSessionId);
		CreateTableData data = new CreateTableData();
		ArrayList<Column> cols = data.columns;
		Column columnId = new Column("ID", Value.INT);
		columnId.setNullable(false);
		cols.add(columnId);
		cols.add(new Column("HEAD", Value.INT));
		cols.add(new Column("TYPE", Value.INT));
		cols.add(new Column("SQL", Value.STRING));
		boolean create = true;

		data.tableName = "SYS";
		data.id = 0;
		data.temporary = false;
		data.persistData = true;
		data.persistIndexes = true;
		data.create = create;
		data.isHidden = true;
		data.session = systemSession;
		meta = mainSchema.createTable(data);
		IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
		metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, IndexType.createPrimaryKey(false, false), true,
				null);
		objectIds.set(0);
		starting = true;
		Cursor cursor = metaIdIndex.find(systemSession, null, null);
		ArrayList<MetaRecord> records = New.arrayList();
		while (cursor.next()) {
			MetaRecord rec = new MetaRecord(cursor.get());
			objectIds.set(rec.getId());
			records.add(rec);
		}
		Collections.sort(records);
		synchronized (systemSession) {
			for (MetaRecord rec : records) {
				rec.execute(this, systemSession);
			}
		}
		recompileInvalidViews(systemSession);
		starting = false;

		getLobStorage().init();
		systemSession.commit(true);

		trace.info("opened {0}", databaseName);
	}

	private void recompileInvalidViews(Session session) {
		boolean atLeastOneRecompiledSuccessfully;
		do {
			atLeastOneRecompiledSuccessfully = false;
			for (Table obj : getAllTablesAndViews(false)) {
				if (obj instanceof TableView) {
					TableView view = (TableView) obj;
					if (view.isInvalid()) {
						view.recompile(session, true, false);
						if (!view.isInvalid()) {
							atLeastOneRecompiledSuccessfully = true;
						}
					}
				}
			}
		} while (atLeastOneRecompiledSuccessfully);
		TableView.clearIndexCaches(session.getDatabase());
	}

	private void initMetaTables() {
		if (metaTablesInitialized) {
			return;
		}
		synchronized (infoSchema) {
			if (!metaTablesInitialized) {
				for (int type = 0, count = MetaTable.getMetaTableTypeCount(); type < count; type++) {
					MetaTable m = new MetaTable(infoSchema, -1 - type, type);
					infoSchema.add(m);
				}
				metaTablesInitialized = true;
			}
		}
	}

	private synchronized void addMeta(Session session, DbObject obj) {
		int id = obj.getId();
		if (id > 0 && !starting && !obj.isTemporary()) {
			Row r = meta.getTemplateRow();
			MetaRecord rec = new MetaRecord(obj);
			rec.setRecord(r);
			objectIds.set(id);
			if (SysProperties.CHECK) {
				verifyMetaLocked(session);
			}
			meta.addRow(session, r);
			if (isMultiVersion()) {
				// TODO this should work without MVCC, but avoid risks at the
				// moment
				session.log(meta, UndoLogRecord.INSERT, r);
			}
		}
	}

	/**
	 * Verify the meta table is locked.
	 *
	 * @param session
	 *            the session
	 */
	public void verifyMetaLocked(Session session) {
		if (meta != null && !meta.isLockedExclusivelyBy(session) && lockMode != Constants.LOCK_MODE_OFF) {
			throw DbException.throwInternalError();
		}
	}

	/**
	 * Lock the metadata table for updates.
	 *
	 * @param session
	 *            the session
	 * @return whether it was already locked before by this session
	 */
	public boolean lockMeta(Session session) {
		// this method can not be synchronized on the database object,
		// as unlocking is also synchronized on the database object -
		// so if locking starts just before unlocking, locking could
		// never be successful
		if (meta == null) {
			return true;
		}
		boolean wasLocked = meta.lock(session, true, true);
		return wasLocked;
	}

	/**
	 * Unlock the metadata table.
	 *
	 * @param session
	 *            the session
	 */
	public void unlockMeta(Session session) {
		meta.unlock(session);
		session.unlock(meta);
	}

	/**
	 * Remove the given object from the meta data.
	 *
	 * @param session
	 *            the session
	 * @param id
	 *            the id of the object to remove
	 */
	public synchronized void removeMeta(Session session, int id) {
		if (id > 0 && !starting) {
			SearchRow r = meta.getTemplateSimpleRow(false);
			r.setValue(0, ValueInt.get(id));
			boolean wasLocked = lockMeta(session);
			Cursor cursor = metaIdIndex.find(session, r, r);
			if (cursor.next()) {
				if (SysProperties.CHECK) {
					if (lockMode != Constants.LOCK_MODE_OFF && !wasLocked) {
						throw DbException.throwInternalError();
					}
				}
				Row found = cursor.get();
				meta.removeRow(session, found);
				if (isMultiVersion()) {
					// TODO this should work without MVCC, but avoid risks at
					// the moment
					session.log(meta, UndoLogRecord.DELETE, found);
				}
				if (SysProperties.CHECK) {
					checkMetaFree(session, id);
				}
			} else if (!wasLocked) {
				// must not keep the lock if it was not locked
				// otherwise updating sequences may cause a deadlock
				meta.unlock(session);
				session.unlock(meta);
			}
			objectIds.clear(id);
		}
	}

	@SuppressWarnings("unchecked")
	private HashMap<String, DbObject> getMap(int type) {
		HashMap<String, ? extends DbObject> result;
		switch (type) {
		case DbObject.USER:
			result = users;
			break;
		case DbObject.SETTING:
			result = settings;
			break;
		case DbObject.ROLE:
			result = roles;
			break;
		case DbObject.RIGHT:
			result = rights;
			break;
		case DbObject.SCHEMA:
			result = schemas;
			break;
		case DbObject.USER_DATATYPE:
			result = userDataTypes;
			break;
		case DbObject.COMMENT:
			result = comments;
			break;
		case DbObject.AGGREGATE:
			result = aggregates;
			break;
		default:
			throw DbException.throwInternalError("type=" + type);
		}
		return (HashMap<String, DbObject>) result;
	}

	/**
	 * Add a schema object to the database.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the object to add
	 */
	public void addSchemaObject(Session session, SchemaObject obj) {
		int id = obj.getId();
		if (id > 0 && !starting) {
			checkWritingAllowed();
		}
		lockMeta(session);
		synchronized (this) {
			obj.getSchema().add(obj);
			addMeta(session, obj);
		}
	}

	/**
	 * Add an object to the database.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the object to add
	 */
	public synchronized void addDatabaseObject(Session session, DbObject obj) {
		int id = obj.getId();
		if (id > 0 && !starting) {
			checkWritingAllowed();
		}
		HashMap<String, DbObject> map = getMap(obj.getType());
		if (obj.getType() == DbObject.USER) {
			User user = (User) obj;
			if (user.isAdmin() && systemUser.getName().equals(SYSTEM_USER_NAME)) {
				systemUser.rename(user.getName());
			}
		}
		String name = obj.getName();
		if (SysProperties.CHECK && map.get(name) != null) {
			DbException.throwInternalError("object already exists");
		}
		lockMeta(session);
		addMeta(session, obj);
		map.put(name, obj);
	}

	/**
	 * Get the user defined aggregate function if it exists, or null if not.
	 *
	 * @param name
	 *            the name of the user defined aggregate function
	 * @return the aggregate function or null
	 */
	public UserAggregate findAggregate(String name) {
		return aggregates.get(name);
	}

	/**
	 * Get the comment for the given database object if one exists, or null if not.
	 *
	 * @param object
	 *            the database object
	 * @return the comment or null
	 */
	public Comment findComment(DbObject object) {
		if (object.getType() == DbObject.COMMENT) {
			return null;
		}
		String key = Comment.getKey(object);
		return comments.get(key);
	}

	/**
	 * Get the role if it exists, or null if not.
	 *
	 * @param roleName
	 *            the name of the role
	 * @return the role or null
	 */
	public Role findRole(String roleName) {
		return roles.get(roleName);
	}

	/**
	 * Get the schema if it exists, or null if not.
	 *
	 * @param schemaName
	 *            the name of the schema
	 * @return the schema or null
	 */
	public Schema findSchema(String schemaName) {
		Schema schema = schemas.get(schemaName);
		if (schema == infoSchema) {
			initMetaTables();
		}
		return schema;
	}

	/**
	 * Get the setting if it exists, or null if not.
	 *
	 * @param name
	 *            the name of the setting
	 * @return the setting or null
	 */
	public Setting findSetting(String name) {
		return settings.get(name);
	}

	/**
	 * Get the user if it exists, or null if not.
	 *
	 * @param name
	 *            the name of the user
	 * @return the user or null
	 */
	public User findUser(String name) {
		return users.get(name);
	}

	/**
	 * Get the user defined data type if it exists, or null if not.
	 *
	 * @param name
	 *            the name of the user defined data type
	 * @return the user defined data type or null
	 */
	public UserDataType findUserDataType(String name) {
		return userDataTypes.get(name);
	}

	/**
	 * Get user with the given name. This method throws an exception if the user
	 * does not exist.
	 *
	 * @param name
	 *            the user name
	 * @return the user
	 * @throws DbException
	 *             if the user does not exist
	 */
	public User getUser(String name) {
		User user = findUser(name);
		if (user == null) {
			throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
		}
		return user;
	}

	/**
	 * Create a session for the given user.
	 *
	 * @param user
	 *            the user
	 * @return the session, or null if the database is currently closing
	 * @throws DbException
	 *             if the database is in exclusive mode
	 */
	public synchronized Session createSession(User user) {
		if (closing) {
			throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
		}
		if (exclusiveSession != null) {
			throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
		}
		Session session = new Session(this, user, ++nextSessionId);
		userSessions.add(session);
		trace.info("connecting session #{0} to {1}", session.getId(), databaseName);
		return session;
	}

	/**
	 * Remove a session. This method is called after the user has disconnected.
	 *
	 * @param session
	 *            the session
	 */
	public synchronized void removeSession(Session session) {
		if (session != null) {
			if (exclusiveSession == session) {
				exclusiveSession = null;
			}
			userSessions.remove(session);
			if (session != systemSession && session != lobSession) {
				trace.info("disconnecting session #{0}", session.getId());
			}
		}
		if (session != systemSession && session != lobSession && session != null) {
			trace.info("disconnected session #{0}", session.getId());
		}
	}

	private synchronized void closeAllSessionsException(Session except) {
		Session[] all = new Session[userSessions.size()];
		userSessions.toArray(all);
		for (Session s : all) {
			if (s != except) {
				try {
					// must roll back, otherwise the session is removed and
					// the transaction log that contains its uncommitted
					// operations as well
					s.rollback();
					s.close();
				} catch (DbException e) {
					trace.error(e, "disconnecting session #{0}", s.getId());
				}
			}
		}
	}

	/**
	 * Close the database.
	 *
	 * @param fromShutdownHook
	 *            true if this method is called from the shutdown hook
	 */
	public void close(boolean fromShutdownHook) {
		synchronized (this) {
			if (closing) {
				return;
			}
			throwLastBackgroundException();

			closing = true;
			if (userSessions.size() > 0) {
				if (!fromShutdownHook) {
					return;
				}
				trace.info("closing {0} from shutdown hook", databaseName);
				closeAllSessionsException(null);
			}
			trace.info("closing {0}", databaseName);
		}
		removeOrphanedLobs();
		try {
			if (systemSession != null) {
				for (SchemaObject obj : getAllSchemaObjects(DbObject.TRIGGER)) {
					TriggerObject trigger = (TriggerObject) obj;
					try {
						trigger.close();
					} catch (SQLException e) {
						trace.error(e, "close");
					}
				}
				meta.close(systemSession);
				systemSession.commit(true);
			}
		} catch (DbException e) {
			trace.error(e, "close");
		}
		tempFileDeleter.deleteAll();
		try {
			closeOpenFilesAndUnlock(true);
		} catch (DbException e) {
			trace.error(e, "close");
		}
		trace.info("closed");
		traceSystem.close();
		if (closeOnExit != null) {
			closeOnExit.reset();
			try {
				Runtime.getRuntime().removeShutdownHook(closeOnExit);
			} catch (IllegalStateException e) {
				// ignore
			} catch (SecurityException e) {
				// applets may not do that - ignore
			}
			closeOnExit = null;
		}
	}

	private void removeOrphanedLobs() {
		try {
			getLobStorage();
			lobStorage.removeAllForTable(LobStorageFrontend.TABLE_ID_SESSION_VARIABLE);
		} catch (DbException e) {
			trace.error(e, "close");
		}
	}

	/**
	 * Close all open files and unlock the database.
	 *
	 * @param flush
	 *            whether writing is allowed
	 */
	private synchronized void closeOpenFilesAndUnlock(boolean flush) {
		deleteOldTempFiles();
		if (systemSession != null) {
			systemSession.close();
			systemSession = null;
		}
		if (lobSession != null) {
			lobSession.close();
			lobSession = null;
		}
	}

 

	private void checkMetaFree(Session session, int id) {
		SearchRow r = meta.getTemplateSimpleRow(false);
		r.setValue(0, ValueInt.get(id));
		Cursor cursor = metaIdIndex.find(session, r, r);
		if (cursor.next()) {
			DbException.throwInternalError();
		}
	}

	/**
	 * Allocate a new object id.
	 *
	 * @return the id
	 */
	public synchronized int allocateObjectId() {
		int i = objectIds.nextClearBit(0);
		objectIds.set(i);
		return i;
	}

	public ArrayList<UserAggregate> getAllAggregates() {
		return New.arrayList(aggregates.values());
	}

	public ArrayList<Comment> getAllComments() {
		return New.arrayList(comments.values());
	}

	public int getAllowLiterals() {
		if (starting) {
			return Constants.ALLOW_LITERALS_ALL;
		}
		return allowLiterals;
	}

	public ArrayList<Right> getAllRights() {
		return New.arrayList(rights.values());
	}

	public ArrayList<Role> getAllRoles() {
		return New.arrayList(roles.values());
	}

	/**
	 * Get all schema objects.
	 *
	 * @return all objects of all types
	 */
	public ArrayList<SchemaObject> getAllSchemaObjects() {
		initMetaTables();
		ArrayList<SchemaObject> list = New.arrayList();
		for (Schema schema : schemas.values()) {
			list.addAll(schema.getAll());
		}
		return list;
	}

	/**
	 * Get all schema objects of the given type.
	 *
	 * @param type
	 *            the object type
	 * @return all objects of that type
	 */
	public ArrayList<SchemaObject> getAllSchemaObjects(int type) {
		if (type == DbObject.TABLE_OR_VIEW) {
			initMetaTables();
		}
		ArrayList<SchemaObject> list = New.arrayList();
		for (Schema schema : schemas.values()) {
			list.addAll(schema.getAll(type));
		}
		return list;
	}

	/**
	 * Get all tables and views.
	 *
	 * @param includeMeta
	 *            whether to force including the meta data tables (if true, metadata
	 *            tables are always included; if false, metadata tables are only
	 *            included if they are already initialized)
	 * @return all objects of that type
	 */
	public ArrayList<Table> getAllTablesAndViews(boolean includeMeta) {
		if (includeMeta) {
			initMetaTables();
		}
		ArrayList<Table> list = New.arrayList();
		for (Schema schema : schemas.values()) {
			list.addAll(schema.getAllTablesAndViews());
		}
		return list;
	}

	/**
	 * Get the tables with the given name, if any.
	 *
	 * @param name
	 *            the table name
	 * @return the list
	 */
	public ArrayList<Table> getTableOrViewByName(String name) {
		ArrayList<Table> list = New.arrayList();
		for (Schema schema : schemas.values()) {
			Table table = schema.getTableOrViewByName(name);
			if (table != null) {
				list.add(table);
			}
		}
		return list;
	}

	public ArrayList<Schema> getAllSchemas() {
		initMetaTables();
		return New.arrayList(schemas.values());
	}

	public ArrayList<Setting> getAllSettings() {
		return New.arrayList(settings.values());
	}

	public ArrayList<UserDataType> getAllUserDataTypes() {
		return New.arrayList(userDataTypes.values());
	}

	public ArrayList<User> getAllUsers() {
		return New.arrayList(users.values());
	}

	public String getCluster() {
		return cluster;
	}

	@Override
	public CompareMode getCompareMode() {
		return compareMode;
	}

	@Override
	public String getDatabasePath() {
		return FileUtils.toRealPath(databaseName);
	}

	public String getShortName() {
		return databaseName;
	}

	public String getName() {
		return databaseName;
	}

	/**
	 * Get all sessions that are currently connected to the database.
	 *
	 * @param includingSystemSession
	 *            if the system session should also be included
	 * @return the list of sessions
	 */
	public Session[] getSessions(boolean includingSystemSession) {
		ArrayList<Session> list;
		// need to synchronized on userSession, otherwise the list
		// may contain null elements
		synchronized (userSessions) {
			list = New.arrayList(userSessions);
		}
		// copy, to ensure the reference is stable
		Session sys = systemSession;
		Session lob = lobSession;
		if (includingSystemSession && sys != null) {
			list.add(sys);
		}
		if (includingSystemSession && lob != null) {
			list.add(lob);
		}
		Session[] array = new Session[list.size()];
		list.toArray(array);
		return array;
	}

	/**
	 * Update an object in the system table.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the database object
	 */
	public void updateMeta(Session session, DbObject obj) {
		lockMeta(session);
		synchronized (this) {
			int id = obj.getId();
			removeMeta(session, id);
			addMeta(session, obj);
			// for temporary objects
			if (id > 0) {
				objectIds.set(id);
			}
		}
	}

	/**
	 * Rename a schema object.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the object
	 * @param newName
	 *            the new name
	 */
	public synchronized void renameSchemaObject(Session session, SchemaObject obj, String newName) {
		checkWritingAllowed();
		obj.getSchema().rename(obj, newName);
		updateMetaAndFirstLevelChildren(session, obj);
	}

	private synchronized void updateMetaAndFirstLevelChildren(Session session, DbObject obj) {
		ArrayList<DbObject> list = obj.getChildren();
		Comment comment = findComment(obj);
		if (comment != null) {
			DbException.throwInternalError(comment.toString());
		}
		updateMeta(session, obj);
		// remember that this scans only one level deep!
		if (list != null) {
			for (DbObject o : list) {
				if (o.getCreateSQL() != null) {
					updateMeta(session, o);
				}
			}
		}
	}

	/**
	 * Rename a database object.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the object
	 * @param newName
	 *            the new name
	 */
	public synchronized void renameDatabaseObject(Session session, DbObject obj, String newName) {
		checkWritingAllowed();
		int type = obj.getType();
		HashMap<String, DbObject> map = getMap(type);
		if (SysProperties.CHECK) {
			if (!map.containsKey(obj.getName())) {
				DbException.throwInternalError("not found: " + obj.getName());
			}
			if (obj.getName().equals(newName) || map.containsKey(newName)) {
				DbException.throwInternalError("object already exists: " + newName);
			}
		}
		obj.checkRename();
		int id = obj.getId();
		lockMeta(session);
		removeMeta(session, id);
		map.remove(obj.getName());
		obj.rename(newName);
		map.put(newName, obj);
		updateMetaAndFirstLevelChildren(session, obj);
	}

	/**
	 * Create a temporary file in the database folder.
	 *
	 * @return the file name
	 */
	public String createTempFile() {
		try {
			String name = databaseName;
			return FileUtils.createTempFile(name, Constants.SUFFIX_TEMP_FILE, true, false);
		} catch (IOException e) {
			throw DbException.convertIOException(e, databaseName);
		}
	}

	private void deleteOldTempFiles() {
		String path = FileUtils.getParent(databaseName);
		for (String name : FileUtils.newDirectoryStream(path)) {
			if (name.endsWith(Constants.SUFFIX_TEMP_FILE) && name.startsWith(databaseName)) {
				// can't always delete the files, they may still be open
				FileUtils.tryDelete(name);
			}
		}
	}

	/**
	 * Get the schema. If the schema does not exist, an exception is thrown.
	 *
	 * @param schemaName
	 *            the name of the schema
	 * @return the schema
	 * @throws DbException
	 *             no schema with that name exists
	 */
	public Schema getSchema(String schemaName) {
		Schema schema = findSchema(schemaName);
		if (schema == null) {
			throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
		}
		return schema;
	}

	/**
	 * Remove the object from the database.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the object to remove
	 */
	public synchronized void removeDatabaseObject(Session session, DbObject obj) {
		checkWritingAllowed();
		String objName = obj.getName();
		int type = obj.getType();
		HashMap<String, DbObject> map = getMap(type);
		if (SysProperties.CHECK && !map.containsKey(objName)) {
			DbException.throwInternalError("not found: " + objName);
		}
		Comment comment = findComment(obj);
		lockMeta(session);
		if (comment != null) {
			removeDatabaseObject(session, comment);
		}
		int id = obj.getId();
		obj.removeChildrenAndResources(session);
		map.remove(objName);
		removeMeta(session, id);
	}

	/**
	 * Get the first table that depends on this object.
	 *
	 * @param obj
	 *            the object to find
	 * @param except
	 *            the table to exclude (or null)
	 * @return the first dependent table, or null
	 */
	public Table getDependentTable(SchemaObject obj, Table except) {
		switch (obj.getType()) {
		case DbObject.COMMENT:
		case DbObject.CONSTRAINT:
		case DbObject.INDEX:
		case DbObject.RIGHT:
		case DbObject.TRIGGER:
		case DbObject.USER:
			return null;
		default:
		}
		HashSet<DbObject> set = New.hashSet();
		for (Table t : getAllTablesAndViews(false)) {
			if (except == t) {
				continue;
			} else if (TableType.VIEW == t.getTableType()) {
				continue;
			}
			set.clear();
			t.addDependencies(set);
			if (set.contains(obj)) {
				return t;
			}
		}
		return null;
	}

	/**
	 * Remove an object from the system table.
	 *
	 * @param session
	 *            the session
	 * @param obj
	 *            the object to be removed
	 */
	public void removeSchemaObject(Session session, SchemaObject obj) {
		int type = obj.getType();
		if (type == DbObject.TABLE_OR_VIEW) {
			Table table = (Table) obj;
			if (table.isTemporary() && !table.isGlobalTemporary()) {
				session.removeLocalTempTable(table);
				return;
			}
		} else if (type == DbObject.INDEX) {
			Index index = (Index) obj;
			Table table = index.getTable();
			if (table.isTemporary() && !table.isGlobalTemporary()) {
				session.removeLocalTempTableIndex(index);
				return;
			}
		} else if (type == DbObject.CONSTRAINT) {
			Constraint constraint = (Constraint) obj;
			Table table = constraint.getTable();
			if (table.isTemporary() && !table.isGlobalTemporary()) {
				session.removeLocalTempTableConstraint(constraint);
				return;
			}
		}
		checkWritingAllowed();
		lockMeta(session);
		synchronized (this) {
			Comment comment = findComment(obj);
			if (comment != null) {
				removeDatabaseObject(session, comment);
			}
			obj.getSchema().remove(obj);
			int id = obj.getId();
			if (!starting) {
				Table t = getDependentTable(obj, null);
				if (t != null) {
					obj.getSchema().add(obj);
					throw DbException.get(ErrorCode.CANNOT_DROP_2, obj.getSQL(), t.getSQL());
				}
				obj.removeChildrenAndResources(session);
			}
			removeMeta(session, id);
		}
	}

	public TraceSystem getTraceSystem() {
		return traceSystem;
	}

	public synchronized void setMasterUser(User user) {
		lockMeta(systemSession);
		addDatabaseObject(systemSession, user);
		systemSession.commit(true);
	}

	public Role getPublicRole() {
		return publicRole;
	}

	/**
	 * Get a unique temporary table name.
	 *
	 * @param baseName
	 *            the prefix of the returned name
	 * @param session
	 *            the session
	 * @return a unique name
	 */
	public synchronized String getTempTableName(String baseName, Session session) {
		String tempName;
		do {
			tempName = baseName + "_COPY_" + session.getId() + "_" + nextTempTableId++;
		} while (mainSchema.findTableOrView(session, tempName) != null);
		return tempName;
	}

	public void setCompareMode(CompareMode compareMode) {
		this.compareMode = compareMode;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	@Override
	public void checkWritingAllowed() {

	}

	public boolean isReadOnly() {
		return false;
	}

 

	private void throwLastBackgroundException() {
		if (backgroundException != null) {
			// we don't care too much about concurrency here,
			// we just want to make sure the exception is _normally_
			// not just logged to the .trace.db file
			DbException b = backgroundException;
			backgroundException = null;
			if (b != null) {
				// wrap the exception, so we see it was thrown here
				throw DbException.get(b.getErrorCode(), b, b.getMessage());
			}
		}
	}

	public void setBackgroundException(DbException e) {
		if (backgroundException == null) {
			backgroundException = e;
			TraceSystem t = getTraceSystem();
			if (t != null) {
				t.getTrace(Trace.DATABASE).error(e, "flush");
			}
		}
	}

	/**
	 * Flush all pending changes to the transaction log.
	 */
	public synchronized void flush() {
	    
	}

	/**
	 * This method is called after an exception occurred, to inform the database
	 * event listener (if one is set).
	 *
	 * @param e
	 *            the exception
	 * @param sql
	 *            the SQL statement
	 */
	public void exceptionThrown(SQLException e, String sql) {

	}

	/**
	 * Synchronize the files with the file system. This method is called when
	 * executing the SQL statement CHECKPOINT SYNC.
	 */
	public synchronized void sync() {
	    
	}

	public int getMaxMemoryRows() {
		return maxMemoryRows;
	}

	public void setMaxMemoryRows(int value) {
		this.maxMemoryRows = value;
	}

	public void setMaxMemoryUndo(int value) {
		this.maxMemoryUndo = value;
	}

	public int getMaxMemoryUndo() {
		return maxMemoryUndo;
	}

	public void setLockMode(int lockMode) {
		switch (lockMode) {
		case Constants.LOCK_MODE_OFF:
			if (multiThreaded) {
				// currently the combination of LOCK_MODE=0 and MULTI_THREADED
				// is not supported. also see code in
				// JdbcDatabaseMetaData#supportsTransactionIsolationLevel(int)
				throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_COMBINATION, "LOCK_MODE=0 & MULTI_THREADED");
			}
			break;
		case Constants.LOCK_MODE_READ_COMMITTED:
		case Constants.LOCK_MODE_TABLE:
		case Constants.LOCK_MODE_TABLE_GC:
			break;
		default:
			throw DbException.getInvalidValueException("lock mode", lockMode);
		}
		this.lockMode = lockMode;
	}

	public int getLockMode() {
		return lockMode;
	}

	public Session getSystemSession() {
		return systemSession;
	}

	/**
	 * Check if the database is in the process of closing.
	 *
	 * @return true if the database is closing
	 */
	public boolean isClosing() {
		return closing;
	}

	public void setMaxLengthInplaceLob(int value) {
		this.maxLengthInplaceLob = value;
	}

	@Override
	public int getMaxLengthInplaceLob() {
		return maxLengthInplaceLob;
	}

	public void setIgnoreCase(boolean b) {
		ignoreCase = b;
	}

	public boolean getIgnoreCase() {
		if (starting) {
			// tables created at startup must not be converted to ignorecase
			return false;
		}
		return ignoreCase;
	}

	@Override
	public String getLobCompressionAlgorithm(int type) {
		return lobCompressionAlgorithm;
	}

	public void setLobCompressionAlgorithm(String stringValue) {
		this.lobCompressionAlgorithm = stringValue;
	}

	public void setAllowLiterals(int value) {
		this.allowLiterals = value;
	}

	public boolean getOptimizeReuseResults() {
		return optimizeReuseResults;
	}

	public void setOptimizeReuseResults(boolean b) {
		optimizeReuseResults = b;
	}

	@Override
	public Object getLobSyncObject() {
		return lobSyncObject;
	}

	public int getSessionCount() {
		return userSessions.size();
	}

	public void setReferentialIntegrity(boolean b) {
		referentialIntegrity = b;
	}

	public boolean getReferentialIntegrity() {
		return referentialIntegrity;
	}

	public void setQueryStatistics(boolean b) {
		queryStatistics = b;
		synchronized (this) {
			if (!b) {
				queryStatisticsData = null;
			}
		}
	}

	public boolean getQueryStatistics() {
		return queryStatistics;
	}

	public void setQueryStatisticsMaxEntries(int n) {
		queryStatisticsMaxEntries = n;
		if (queryStatisticsData != null) {
			synchronized (this) {
				if (queryStatisticsData != null) {
					queryStatisticsData.setMaxQueryEntries(queryStatisticsMaxEntries);
				}
			}
		}
	}

	public QueryStatisticsData getQueryStatisticsData() {
		if (!queryStatistics) {
			return null;
		}
		if (queryStatisticsData == null) {
			synchronized (this) {
				if (queryStatisticsData == null) {
					queryStatisticsData = new QueryStatisticsData(queryStatisticsMaxEntries);
				}
			}
		}
		return queryStatisticsData;
	}

	/**
	 * Check if the database is currently opening. This is true until all stored SQL
	 * statements have been executed.
	 *
	 * @return true if the database is still starting
	 */
	public boolean isStarting() {
		return starting;
	}

	/**
	 * Check if multi version concurrency is enabled for this database.
	 *
	 * @return true if it is enabled
	 */
	public boolean isMultiVersion() {
		return multiVersion;
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public Mode getMode() {
		return mode;
	}

	public boolean isMultiThreaded() {
		return multiThreaded;
	}

	public void setMultiThreaded(boolean multiThreaded) {
		this.multiThreaded = multiThreaded;
	}

	public void setMaxOperationMemory(int maxOperationMemory) {
		this.maxOperationMemory = maxOperationMemory;
	}

	public int getMaxOperationMemory() {
		return maxOperationMemory;
	}

	public Session getExclusiveSession() {
		return exclusiveSession;
	}

	/**
	 * Set the session that can exclusively access the database.
	 *
	 * @param session
	 *            the session
	 * @param closeOthers
	 *            whether other sessions are closed
	 */
	public void setExclusiveSession(Session session, boolean closeOthers) {
		this.exclusiveSession = session;
		if (closeOthers) {
			closeAllSessionsException(session);
		}
	}

	@Override
	public SmallLRUCache<String, String[]> getLobFileListCache() {
		if (lobFileListCache == null) {
			lobFileListCache = SmallLRUCache.newInstance(128);
		}
		return lobFileListCache;
	}

	/**
	 * Checks if the system table (containing the catalog) is locked.
	 *
	 * @return true if it is currently locked
	 */
	public boolean isSysTableLocked() {
		return meta == null || meta.isLockedExclusively();
	}

	/**
	 * Checks if the system table (containing the catalog) is locked by the given
	 * session.
	 *
	 * @param session
	 *            the session
	 * @return true if it is currently locked
	 */
	public boolean isSysTableLockedBy(Session session) {
		return meta == null || meta.isLockedExclusivelyBy(session);
	}

	/**
	 * Immediately close the database.
	 */
	public void shutdownImmediately() {
		try {
			checkPowerOff();
		} catch (DbException e) {
			// ignore
		}
	}

	@Override
	public TempFileDeleter getTempFileDeleter() {
		return tempFileDeleter;
	}

	/**
	 * Get the first user defined table.
	 *
	 * @return the table or null if no table is defined
	 */
	public Table getFirstUserTable() {
		for (Table table : getAllTablesAndViews(false)) {
			if (table.getCreateSQL() != null) {
				if (table.isHidden()) {
					// LOB tables
					continue;
				}
				return table;
			}
		}
		return null;
	}

	/**
	 * This method is called before writing to the transaction log.
	 *
	 * @return true if the call was successful and writing is allowed, false if
	 *         another connection was faster
	 */
	public boolean beforeWriting() {
		return false;
	}

	/**
	 * This method is called after updates are finished.
	 */
	public void afterWriting() {

	}

	public void setCompactMode(int compactMode) {
		this.compactMode = compactMode;
	}

	public SourceCompiler getCompiler() {
		if (compiler == null) {
			compiler = new SourceCompiler();
		}
		return compiler;
	}

	@Override
	public LobStorageInterface getLobStorage() {
		if (lobStorage == null) {
            lobStorage = new LobStorageMap(this);
		}
		return lobStorage;
	}

	public Session getLobSession() {
		return lobSession;
	}

	public int getDefaultTableType() {
		return defaultTableType;
	}

	public void setDefaultTableType(int defaultTableType) {
		this.defaultTableType = defaultTableType;
	}

	public void setMultiVersion(boolean multiVersion) {
		this.multiVersion = multiVersion;
	}

	public DbSettings getSettings() {
		return dbSettings;
	}

	/**
	 * Create a new hash map. Depending on the configuration, the key is case
	 * sensitive or case insensitive.
	 *
	 * @param <V>
	 *            the value type
	 * @return the hash map
	 */
	public <V> HashMap<String, V> newStringMap() {
		return dbSettings.databaseToUpper ? new HashMap<String, V>() : new CaseInsensitiveMap<V>();
	}

	/**
	 * Create a new hash map. Depending on the configuration, the key is case
	 * sensitive or case insensitive.
	 *
	 * @param <V>
	 *            the value type
	 * @return the hash map
	 */
	public <V> ConcurrentHashMap<String, V> newConcurrentStringMap() {
		return dbSettings.databaseToUpper ? new NullableKeyConcurrentMap<V>() : new CaseInsensitiveConcurrentMap<V>();
	}

	/**
	 * Compare two identifiers (table names, column names,...) and verify they are
	 * equal. Case sensitivity depends on the configuration.
	 *
	 * @param a
	 *            the first identifier
	 * @param b
	 *            the second identifier
	 * @return true if they match
	 */
	public boolean equalsIdentifiers(String a, String b) {
		if (a == b || a.equals(b)) {
			return true;
		}
		if (!dbSettings.databaseToUpper && a.equalsIgnoreCase(b)) {
			return true;
		}
		return false;
	}

	@Override
	public int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
		throw DbException.throwInternalError();
	}

	public byte[] getFileEncryptionKey() {
		return fileEncryptionKey;
	}

	@Override
	public JavaObjectSerializer getJavaObjectSerializer() {
		initJavaObjectSerializer();
		return javaObjectSerializer;
	}

	private void initJavaObjectSerializer() {
		if (javaObjectSerializerInitialized) {
			return;
		}
		synchronized (this) {
			if (javaObjectSerializerInitialized) {
				return;
			}
			String serializerName = javaObjectSerializerName;
			if (serializerName != null) {
				serializerName = serializerName.trim();
				if (!serializerName.isEmpty() && !serializerName.equals("null")) {
					try {
						javaObjectSerializer = (JavaObjectSerializer) JdbcUtils.loadUserClass(serializerName)
								.newInstance();
					} catch (Exception e) {
						throw DbException.convert(e);
					}
				}
			}
			javaObjectSerializerInitialized = true;
		}
	}

	public void setJavaObjectSerializerName(String serializerName) {
		synchronized (this) {
			javaObjectSerializerInitialized = false;
			javaObjectSerializerName = serializerName;
		}
	}

	/**
	 * Get the table engine class, loading it if needed.
	 *
	 * @param tableEngine
	 *            the table engine name
	 * @return the class
	 */
	public TableEngine getTableEngine(String tableEngine) {
		assert Thread.holdsLock(this);

		TableEngine engine = tableEngines.get(tableEngine);
		if (engine == null) {
			try {
				engine = (TableEngine) JdbcUtils.loadUserClass(tableEngine).newInstance();
			} catch (Exception e) {
				throw DbException.convert(e);
			}
			tableEngines.put(tableEngine, engine);
		}
		return engine;
	}

}
