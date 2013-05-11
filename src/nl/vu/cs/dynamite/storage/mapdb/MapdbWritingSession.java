package nl.vu.cs.dynamite.storage.mapdb;

import java.util.Arrays;

import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.mapdb.BTreeMap;
import org.mapdb.DB;

public class MapdbWritingSession implements WritingSession {

	private final BTreeMap<byte[], byte[]> index;
	private final DB db;

	public MapdbWritingSession(BTreeMap<byte[], byte[]> index, DB db) {
		this.index = index;
		this.db = db;
	}

	@Override
	public void close() {
		// db.commit();
	}

	@Override
	public boolean decreaseOrRemove(byte[] key, int keyLen, int count) {
		// Need immutable key for MapDB.
		key = Arrays.copyOf(key, keyLen);
		synchronized (index) {
			byte[] result = index.get(key);
			if (result == null) {
				return false;
			}

			int c = Utils.decodeInt(result, 4) - count;
			if (c > 0) {
				Utils.encodeInt(result, 4, c);
				index.put(key, result);
				return false;
			} else {
				index.remove(key);
				return true;
			}
		}
	}

	@Override
	public int write(byte[] key, int keyLen, byte[] value) throws Exception {
		// Need immutable key/value for MapDB.
		key = Arrays.copyOf(key, keyLen);
		value = Arrays.copyOf(value, value.length);
		synchronized (index) {
			byte[] result = index.putIfAbsent(key, value);
			if (result != null) {
				return WritingSession.ALREADY_EXISTING;
			}
			return WritingSession.SUCCESS;
		}
	}

	@Override
	public int writeWithCount(byte[] key, int keyLen, byte[] value, int c,
			boolean override) throws Exception {
		// Need immutable key/value for MapDB.
		key = Arrays.copyOf(key, keyLen);
		value = Arrays.copyOf(value, value.length);
		byte[] result;
		synchronized (index) {
			if (override || (result = index.get(key)) == null) {
				Utils.encodeInt(value, 4, c);
			} else {
				int previousCount = Utils.decodeInt(result, 4);
				Utils.encodeInt(value, 4, previousCount + c);
			}

			result = index.put(key, value);
		}
		return result == null ? WritingSession.SUCCESS
				: WritingSession.ALREADY_EXISTING;
	}

	@Override
	public int writeKey(byte[] key, int keyLen) {
		key = Arrays.copyOf(key, keyLen);
		synchronized (index) {
			byte[] text = index.putIfAbsent(key, new byte[8]);

			if (text == null) {
				return WritingSession.SUCCESS;
			}
			return WritingSession.ALREADY_EXISTING;
		}
	}

	@Override
	public int removeIfStepNonZero(byte[] key, int keyLen) {
		key = Arrays.copyOf(key, keyLen);
		synchronized (index) {
			byte[] result = index.get(key);
			if (result == null) {
				return -1;
			}

			int step = Utils.decodeInt(result, 0);
			if (step != 0) {
				index.remove(key);
				return step;
			} else {
				return 0;
			}
		}
	}

	@Override
	public int remove(byte[] key, int keyLen) {
		if (keyLen != key.length) {
			key = Arrays.copyOf(key, keyLen);
		}
		synchronized (index) {
			byte[] result = index.get(key);
			if (result == null) {
				return -1;
			}

			int step = Utils.decodeInt(result, 0);
			index.remove(key);
			return step;
		}
	}

	@Override
	public byte[] get(byte[] key, int keyLen) {
		key = Arrays.copyOf(key, keyLen);
		synchronized (index) {
			return index.get(key);
		}
	}
}
