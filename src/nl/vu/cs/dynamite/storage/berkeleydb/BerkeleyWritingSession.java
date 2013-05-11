package nl.vu.cs.dynamite.storage.berkeleydb;

import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

public class BerkeleyWritingSession implements WritingSession {

	protected static final Logger log = LoggerFactory
			.getLogger(BerkeleyWritingSession.class);

	private final Database db;
	private final DatabaseEntry entryValue = new DatabaseEntry();
	private final DatabaseEntry entryKey = new DatabaseEntry();

	private final boolean compressedKeys;

	public BerkeleyWritingSession(Database db, boolean compressedKeys) {
		this.db = db;
		this.compressedKeys = compressedKeys;
	}

	@Override
	public void close() {
	}

	@Override
	public boolean decreaseOrRemove(byte[] key, int keyLen, int count) {
		entryKey.setData(key, 0, keyLen);
		return decreaseOrRemove(count);
	}

	private boolean decreaseOrRemove(int count) {
		OperationStatus op = db.get(null, entryKey, entryValue, null);
		if (op == OperationStatus.NOTFOUND) {
			return false;
		}

		byte[] value = entryValue.getData();
		int c = Utils.decodeInt(value, 4) - count;
		if (c > 0) {
			if (log.isDebugEnabled() && !compressedKeys) {
				byte[] key = entryKey.getData();
				log.debug("DB " + db.getDatabaseName() + ". The triple "
						+ Utils.decodeLong(key, 0) + " "
						+ Utils.decodeLong(key, 8) + " "
						+ Utils.decodeLong(key, 16) + " is not removed. Step="
						+ Utils.decodeInt(value, 0) + " PC="
						+ Utils.decodeInt(value, 4) + " NC=" + c);
			}

			Utils.encodeInt(value, 4, c);
			entryValue.setData(value); // otherwise no-op because getValue()
										// returns a copy.
			synchronized (db) {
				db.put(null, entryKey, entryValue);
			}
			return false;
		} else {
			synchronized (db) {
				db.delete(null, entryKey);
			}
			return true;
		}
	}

	@Override
	public int write(byte[] key, int keyLen, byte[] value) throws Exception {
		entryKey.setData(key, 0, keyLen);
		entryValue.setData(value);
		return write();
	}

	private int write() {
		OperationStatus op;

		synchronized (db) {
			op = db.putNoOverwrite(null, entryKey, entryValue);
		}
		if (op == OperationStatus.SUCCESS) {
			return WritingSession.SUCCESS;
		} else if (op == OperationStatus.KEYEXIST) {
			return WritingSession.ALREADY_EXISTING;
		} else
			return WritingSession.ERROR;
	}

	@Override
	public int writeWithCount(byte[] key, int keyLen, byte[] value, int c,
			boolean override) throws Exception {
		entryKey.setData(key, 0, keyLen);
		return writeWithCount(value, c, override);
	}

	private int writeWithCount(byte[] value, int c, boolean override) {
		synchronized (db) {
			OperationStatus op = db.get(null, entryKey, entryValue, null);
			if (override || op == OperationStatus.NOTFOUND) {
				Utils.encodeInt(value, 4, c);
			} else {
				value = entryValue.getData();
				int previousCount = Utils.decodeInt(value, 4);
				Utils.encodeInt(value, 4, previousCount + c);
			}
			entryValue.setData(value);
			OperationStatus newOp;

			newOp = db.put(null, entryKey, entryValue);

			if (newOp == OperationStatus.SUCCESS) {
				return op == OperationStatus.NOTFOUND ? WritingSession.SUCCESS
						: WritingSession.ALREADY_EXISTING;
			} else
				return WritingSession.ERROR;
		}
	}

	@Override
	public int writeKey(byte[] key, int keyLen) {
		entryKey.setData(key, 0, keyLen);
		return writeKey();
	}

	private int writeKey() {
		OperationStatus op;
		synchronized (db) {
			op = db.putNoOverwrite(null, entryKey, entryValue);
		}
		if (op == OperationStatus.SUCCESS) {
			return WritingSession.SUCCESS;
		} else if (op == OperationStatus.KEYEXIST) {
			return WritingSession.ALREADY_EXISTING;
		}
		return WritingSession.ERROR;
	}

	@Override
	public int removeIfStepNonZero(byte[] key, int keyLen) {
		entryKey.setData(key, 0, keyLen);
		synchronized (db) {
			OperationStatus op = db.get(null, entryKey, entryValue, null);
			if (op == OperationStatus.NOTFOUND) {
				return -1;
			}
			byte[] value = entryValue.getData();
			int step = Utils.decodeInt(value, 0);
			if (log.isDebugEnabled()) {
				log.debug("removeIfStepNonZero: step = " + step);
			}
			if (step != 0) {
				db.delete(null, entryKey);
				return step;
			} else {
				return 0;
			}
		}
	}

	@Override
	public int remove(byte[] key, int keyLen) {
		entryKey.setData(key, 0, keyLen);
		synchronized (db) {
			OperationStatus op = db.get(null, entryKey, entryValue, null);
			if (op == OperationStatus.NOTFOUND) {
				return -1;
			}
			byte[] value = entryValue.getData();
			int step = Utils.decodeInt(value, 0);
			db.delete(null, entryKey);
			return step;
		}
	}

	@Override
	public byte[] get(byte[] key, int keyLen) {
		entryKey.setData(key, 0, keyLen);
		OperationStatus op = db.get(null, entryKey, entryValue, null);
		if (op == OperationStatus.NOTFOUND) {
			return null;
		}

		return entryValue.getData();
	}
}
