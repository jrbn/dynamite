package nl.vu.cs.dynamite.storage.berkeleydb;

import java.io.File;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.DBType;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;

public class BerkeleydbLayer extends InputLayer implements BTreeInterface {

	static final Logger log = LoggerFactory.getLogger(BerkeleydbLayer.class);

	final public static String DB_INPUT = "berkeleydb.inputpath";
	final public static String COMPRESS_KEYS = "berkeleydb.compress";

	private static Environment env = null;
	private static Database spo = null;
	private static Database sop = null;
	private static Database pos = null;
	private static Database pso = null;
	private static Database ops = null;
	private static Database osp = null;
	private static Database t2n = null;
	private static Database n2t = null;

	private DatabaseConfig dbConfig;
	private boolean isInitialized = false;
	private boolean compressedKeys;
	
	private static int counter = 0;

	public static void setInputDir(Configuration conf, String inputDir) {
		conf.set(DB_INPUT, inputDir);
	}

	private void configureEnvironment(EnvironmentConfig envConfig) {
		envConfig.setAllowCreate(true);
		envConfig.setAllowCreateVoid(true);
		envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "200000000");
		envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
		envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,
				"false");
		envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,
				"false");
	}

	private void configureDatabases(DatabaseConfig dbConfig) {
		dbConfig.setDeferredWrite(true);
		dbConfig.setKeyPrefixing(true);
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);
		dbConfig.setAllowCreateVoid(true);
	}

	private Database loadDb(DBType db) throws Exception {
		synchronized (BerkeleydbLayer.class) {
			Database database = fromDbToObj(db);
			if (database == null) {
				database = env.openDatabase(null, fromDbToName(db), dbConfig);
				initDatabase(db, database);
			}
			return database;
		}
	}

	private void initDatabase(DBType type, Database database) {
		switch (type) {
		case SPO:
			spo = database;
			break;
		case SOP:
			sop = database;
			break;
		case POS:
			pos = database;
			break;
		case PSO:
			pso = database;
			break;
		case OSP:
			osp = database;
			break;
		case OPS:
			ops = database;
			break;
		case T2N:
			t2n = database;
			break;
		case N2T:
			n2t = database;
			break;
		}
	}

	private Database fromDbToObj(DBType db) {
		switch (db) {
		case SPO:
			return spo;
		case SOP:
			return sop;
		case POS:
			return pos;
		case PSO:
			return pso;
		case OSP:
			return osp;
		case OPS:
			return ops;
		case T2N:
			return t2n;
		case N2T:
			return n2t;
		}
		return null;
	}

	private String fromDbToName(DBType db) {
		switch (db) {
		case SPO:
			return "spo";
		case SOP:
			return "sop";
		case POS:
			return "pos";
		case PSO:
			return "pso";
		case OSP:
			return "osp";
		case OPS:
			return "ops";
		case T2N:
			return "t2n";
		case N2T:
			return "n2t";
		}
		return null;
	}

	private void initEnvironment(String inputDir, boolean compressKeys) {
		synchronized (BerkeleydbLayer.class) {
			if (env == null) {
				if (env == null) {
					EnvironmentConfig envConfig = new EnvironmentConfig();
					configureEnvironment(envConfig);
					env = new Environment(new File(inputDir), envConfig);
				}
			}		

			if (!isInitialized) {
				dbConfig = new DatabaseConfig();
				configureDatabases(dbConfig);
				isInitialized = true;
				this.compressedKeys = compressKeys;
				counter++;
			}
		}
	}

	@Override
	protected void load(Context context) throws Exception {
		String inputDir = context.getConfiguration().get(DB_INPUT, null);
		boolean compressKeys = context.getConfiguration().getBoolean(
				COMPRESS_KEYS, false);
		initEnvironment(inputDir, compressKeys);
		spo = loadDb(DBType.SPO);
		sop = loadDb(DBType.SOP);
		pos = loadDb(DBType.POS);
		pso = loadDb(DBType.PSO);
		ops = loadDb(DBType.OPS);
		osp = loadDb(DBType.OSP);
	}

	@Override
	public int encode(byte[] triple, long l1, long l2, long l3) {
		if (compressedKeys) {
			int position = Utils.encodePackedLong(triple, 0, l1);
			position = Utils.encodePackedLong(triple, position, l2);
			position = Utils.encodePackedLong(triple, position, l3);
			return position;
		} else {
			Utils.encodeLong(triple, 0, l1);
			Utils.encodeLong(triple, 8, l2);
			Utils.encodeLong(triple, 16, l3);
			return 24;
		}
	}

	@Override
	public void remove(Tuple tuple) {
		byte[] triple = new byte[24];

		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();

		int sz = encode(triple, s, p, o);
		DatabaseEntry entry = new DatabaseEntry(triple, 0, sz);

		spo.delete(null, entry);
		sop.delete(null, entry);
		pos.delete(null, entry);
		pso.delete(null, entry);
		ops.delete(null, entry);
		osp.delete(null, entry);
	}

	@Override
	public void decreaseOrRemove(Tuple tuple, int count) {
		byte[] triple = new byte[24];
		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();

		int sz = encode(triple, s, p, o);
		DatabaseEntry entry = new DatabaseEntry(triple, 0, sz);

		byte[] bValue = new byte[8];
		DatabaseEntry entryValue = new DatabaseEntry(bValue);

		OperationStatus op = spo.get(null, entry, entryValue, null);
		if (op == OperationStatus.NOTFOUND) {
			return;
		}

		int c = Utils.decodeInt(entryValue.getData(), 4) - count;
		if (c > 0) {
			Utils.encodeInt(entryValue.getData(), 4, c);
			spo.put(null, entry, entryValue);
			sop.put(null, entry, entryValue);
			pos.put(null, entry, entryValue);
			pso.put(null, entry, entryValue);
			ops.put(null, entry, entryValue);
			osp.put(null, entry, entryValue);
		} else {
			spo.delete(null, entry);
			sop.delete(null, entry);
			pos.delete(null, entry);
			pso.delete(null, entry);
			ops.delete(null, entry);
			osp.delete(null, entry);
		}
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();

		// Key to access the entries in the db (initialized to 0)
		byte[] triple = new byte[24];
		// If not entering in any of the branches below, constantBytes remains 0
		int constantBytes = 0;
		DBType dbType = DBType.SPO;
		// o is a variable
		if (o < 0) {
			if (p < 0) {
				if (s >= 0) {
					dbType = DBType.SPO;
					if (compressedKeys) {
						constantBytes = Utils.encodePackedLong(triple, 0, s);
					} else {
						constantBytes = 8;
						Utils.encodeLong(triple, 0, s);
					}
				}
			} else if (s < 0) {
				dbType = DBType.PSO;
				if (compressedKeys) {
					constantBytes = Utils.encodePackedLong(triple, 0, p);
				} else {
					constantBytes = 8;
					Utils.encodeLong(triple, 0, p);
				}
			} else {
				dbType = DBType.SPO;
				if (compressedKeys) {
					constantBytes = Utils.encodePackedLong(triple, 0, s);
					constantBytes = Utils.encodePackedLong(triple,
							constantBytes, p);
				} else {
					constantBytes = 16;
					Utils.encodeLong(triple, 0, s);
					Utils.encodeLong(triple, 8, p);
				}
			}
		}
		// p is a variable
		else if (p < 0) {
			if (s < 0) {
				dbType = DBType.OSP;
				if (compressedKeys) {
					constantBytes = Utils.encodePackedLong(triple, 0, o);
				} else {
					constantBytes = 8;
					Utils.encodeLong(triple, 0, o);
				}
			} else {
				dbType = DBType.OSP;
				if (compressedKeys) {
					constantBytes = Utils.encodePackedLong(triple, 0, o);
					constantBytes = Utils.encodePackedLong(triple,
							constantBytes, s);
				} else {
					constantBytes = 16;
					Utils.encodeLong(triple, 0, o);
					Utils.encodeLong(triple, 8, s);
				}
			}
		}
		// s is a varibale
		else if (s < 0) {
			dbType = DBType.OPS;
			if (compressedKeys) {
				constantBytes = Utils.encodePackedLong(triple, 0, o);
				constantBytes = Utils
						.encodePackedLong(triple, constantBytes, p);
			} else {
				constantBytes = 16;
				Utils.encodeLong(triple, 0, o);
				Utils.encodeLong(triple, 8, p);
			}
		}
		// Completely specified triple
		else {
			dbType = DBType.SPO;
			if (compressedKeys) {
				constantBytes = Utils.encodePackedLong(triple, 0, s);
				constantBytes = Utils
						.encodePackedLong(triple, constantBytes, p);
				constantBytes = Utils
						.encodePackedLong(triple, constantBytes, o);
			} else {
				constantBytes = 24;
				Utils.encodeLong(triple, 0, s);
				Utils.encodeLong(triple, 8, p);
				Utils.encodeLong(triple, 16, o);
			}
		}
		Database db = fromDbToObj(dbType);
		DatabaseEntry key = new DatabaseEntry(triple, 0, constantBytes);

		TupleIterator itr = null;
		if (tuple.getNElements() == 3) {
			itr = new BerkeleydbIterator(key, constantBytes, db, dbType,
					compressedKeys);
		} else {
			itr = new BerkeleydbIterator(key, constantBytes, db, dbType,
					compressedKeys, ((TInt) tuple.get(3)).getValue(),
					((TInt) tuple.get(4)).getValue());
		}
		itr.init(context, "BerkeleyDB");
		return itr;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		// Nothing to do
	}

	@Override
	public Location getLocations(Tuple tuple, ActionContext context) {
		// For now it supports only a local machine.
		return Location.THIS_NODE;
	}

	@Override
	public WritingSession openWritingSession(ActionContext context, DBType type) throws Exception {
		if (!isInitialized) {
			initEnvironment(context.getSystemParamString(DB_INPUT, null),
					context.getSystemParamBoolean(COMPRESS_KEYS, false));
		}
		return new BerkeleyWritingSession(loadDb(type), compressedKeys);
	}

	public Database openDatabase(ActionContext context, String databaseName) {
		if (!isInitialized) {
			initEnvironment(context.getSystemParamString(DB_INPUT, null),
					context.getSystemParamBoolean(COMPRESS_KEYS, false));
		}
		return env.openDatabase(null, databaseName, dbConfig);
	}

	@Override
	public void close() {
		synchronized(this.getClass()) {
			if (! isInitialized) {
				return;
			}
			isInitialized = true;
			counter--;
			if (counter != 0) {
				return;
			}
	
			IfOpencloseDB();
		}
	}

	private static void IfOpencloseDB() {
		if (spo != null) {
			spo.close();
			spo = null;
		}
		if (sop != null) {
			sop.close();
			sop = null;
		}
		if (pos != null) {
			pos.close();
			pos = null;
		}
		if (pso != null) {
			pso.close();
			pso = null;
		}
		if (ops != null) {
			ops.close();
			ops = null;
		}
		if (osp != null) {
			osp.close();
			osp = null;
		}
		if (t2n != null) {
			t2n.close();
			t2n = null;
		}
		if (n2t != null) {
			n2t.close();
			n2t = null;
		}
		if (log.isDebugEnabled() && env.isValid()) {
			StatsConfig config = new StatsConfig();
			config.setClear(true);

			log.debug(env.getStats(config).toString());
		}
		if (env != null) {
			env.close();
			env = null;
		}
	}
}
