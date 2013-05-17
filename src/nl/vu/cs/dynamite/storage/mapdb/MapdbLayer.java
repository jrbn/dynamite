package nl.vu.cs.dynamite.storage.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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
import nl.vu.cs.ajira.utils.Utils.BytesComparator;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.DBType;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapdbLayer extends InputLayer implements BTreeInterface {

	private static class ByteArraySerializer extends BTreeKeySerializer<byte[]> implements Serializer<byte[]>, java.io.Serializable {

		@Override
		public byte[] deserialize(DataInput in, int available) throws IOException {
			int n = in.readByte();
			if (n == -1) {
				return null;
			}
			byte[] b = new byte[n];
			if (n > 0) {
				in.readFully(b);
			}
			return b;
		}

		@Override
		public void serialize(DataOutput out, byte[] b) throws IOException {
			if (b == null) {
				out.writeByte(-1);
			} else {
				out.writeByte(b.length);
				if (b.length > 0) {
					out.write(b);
				}	
			}
		}
		
		@Override
		public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
			for (int i = start; i < end; i++) {
				byte[] b = ((byte[]) keys[i]);
				serialize(out, b);
			}
		}

		@Override
		public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
			Object[] ret = new Object[size];
			for (int i = start; i < end; i++) {
				ret[i] = deserialize(in, 0);
			}
			return ret;
		}
	}

	private static final ByteArraySerializer serializer = new ByteArraySerializer();
	
	private static final BytesComparator cmp = new BytesComparator();
	
	static final Logger log = LoggerFactory.getLogger(MapdbLayer.class);

	final public static String DB_INPUT = "mapdb.inputpath";

	private String inputDir = null;
	private static DB db = null;
	private static BTreeMap<byte[], byte[]> spo = null;
	private static BTreeMap<byte[], byte[]> sop = null;
	private static BTreeMap<byte[], byte[]> pos = null;
	private static BTreeMap<byte[], byte[]> pso = null;
	private static BTreeMap<byte[], byte[]> ops = null;
	private static BTreeMap<byte[], byte[]> osp = null;
	private static BTreeMap<byte[], byte[]> t2n = null;
	private static BTreeMap<byte[], byte[]> n2t = null;
	
	private boolean compressedKeys;
	private boolean isInitialized = false;
	
	private static int counter = 0;

	public static void setInputDir(Configuration conf, String inputDir) {
		conf.set(DB_INPUT, inputDir);
	}
	
	private void init(Context context) throws Exception {
		synchronized(this.getClass()) {
			if (db == null) {
				inputDir = context.getConfiguration().get(DB_INPUT, null);
				if (inputDir == null) {
					throw new Exception("Input directory not found");
				}

				DBMaker maker = DBMaker.newFileDB(new File(inputDir, "MAPDB"))
						.closeOnJvmShutdown().writeAheadLogDisable()
						.asyncFlushDelay(10);
				if (Runtime.getRuntime().maxMemory() < 1L * 1024 * 1024 * 1024) {
					maker = maker.randomAccessFileEnable();
				} else {
					maker = maker.randomAccessFileEnableIfNeeded();
				}
				db = maker.make();
			}
			if (! isInitialized) {
				counter++;
				isInitialized = true;
			}
		}
	}

	@Override
	protected void load(Context context) throws Exception {
		init(context);

		// Look in the "Config" database to see if keys are compressed.
		HTreeMap<String, Boolean> c = db.getHashMap("Config");
		compressedKeys = c.get("CompressedKeys");

		if (log.isDebugEnabled()) {
			log.debug("Compressed keys: " + compressedKeys);
		}

		spo = loadDb(context, DBType.SPO);
		sop = loadDb(context, DBType.SOP);
		pos = loadDb(context, DBType.POS);
		pso = loadDb(context, DBType.PSO);
		ops = loadDb(context, DBType.OPS);
		osp = loadDb(context, DBType.OSP);

		if (log.isDebugEnabled()) {
			log.debug("SPO size = " + spo.size());
		}
	}

	@Override
	public void remove(Tuple tuple) {
		byte[] triple = new byte[24];
		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();

		int sz = encode(triple, s, p, o);
		if (sz < triple.length) {
			triple = Arrays.copyOf(triple, sz);
		}

		spo.remove(triple);
		sop.remove(triple);
		pos.remove(triple);
		pso.remove(triple);
		ops.remove(triple);
		osp.remove(triple);
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
		BTreeMap<byte[], byte[]> index = fromDbToObj(dbType);

		TupleIterator itr = null;
		if (tuple.getNElements() == 3) {
			itr = new MapdbIterator(triple, constantBytes, index, dbType,
					compressedKeys);
		} else {
			itr = new MapdbIterator(triple, constantBytes, index, dbType,
					compressedKeys, ((TInt) tuple.get(3)).getValue(),
					((TInt) tuple.get(4)).getValue());
		}
		itr.init(context, "MapDB");
		return itr;
	}
	
	private BTreeMap<byte[], byte[]> loadDb(Context context, DBType dbType) throws Exception {
		init(context);
		synchronized (MapdbLayer.class) {
			BTreeMap<byte[], byte[]> database = fromDbToObj(dbType);
			if (database == null) {
				try {
					database = db.createTreeMap(fromDbToName(dbType), 64, false, false, serializer, serializer, cmp);
				} catch(IllegalArgumentException e) {
					// it already exists
					database = db.getTreeMap(fromDbToName(dbType));
				}
				initDatabase(dbType, database);
			}
			return database;
		}
	}


	private BTreeMap<byte[], byte[]> fromDbToObj(DBType db) {
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
	
	private void initDatabase(DBType type, BTreeMap<byte[], byte[]> database) {
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
		BTreeMap<byte[], byte[]> index = loadDb(context.getContext(), type);
		return new MapdbWritingSession(index, db);

	}

	@Override
	public void close() {
		if (! isInitialized) {
			return;
		}
		isInitialized = false;
		synchronized(this.getClass()) {
			counter--;
			if (counter > 0) {
				return;
			}

			if (db != null) {
				db.commit();
				db.close();
				db = null;
			}
		}
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
	public void decreaseOrRemove(Tuple tuple, int count) {
		log.error("Not implemented!!!!", new Throwable());
		System.exit(1);
	}
}
