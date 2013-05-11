package nl.vu.cs.dynamite.storage.mapdb;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.mapdb.BTreeMap;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.ajira.utils.Utils.BytesComparator;
import nl.vu.cs.dynamite.storage.DBType;

public class MapdbIterator extends TupleIterator {

	private static final BytesComparator cmp = new BytesComparator();

	private byte[] value;
	private byte[] key;
	private Entry<byte[], byte[]> entry;
	private final int constantBytes;
	private final DBType type;

	private boolean first;
	private boolean closed;

	private final byte[] prefixKey;
	private final boolean compressedKeys;
	private int start = 0;
	private int offset = 0;
	private final int[] position = new int[1];
	Iterator<Entry<byte[], byte[]>> cursor = null;
	private final BTreeMap<byte[], byte[]> db;
	private SimpleData[] signature;
	private TLong l1, l2, l3;
	private TInt step;

	MapdbIterator(byte[] key, int constantBytes, BTreeMap<byte[], byte[]> db,
			DBType type, boolean compressedKeys) {
		this.db = db;
		this.constantBytes = constantBytes;
		this.type = type;
		this.compressedKeys = compressedKeys;
		first = true;
		closed = false;
		prefixKey = Arrays.copyOf(key, constantBytes);
		signature = new SimpleData[4];
		signature[0] = l1 = new TLong();
		signature[1] = l2 = new TLong();
		signature[2] = l3 = new TLong();
		signature[3] = step = new TInt();
	}

	MapdbIterator(byte[] key, int constantBytes, BTreeMap<byte[], byte[]> db,
			DBType dbType, boolean compressedKeys, int start, int offset) {
		this(key, constantBytes, db, dbType, compressedKeys);
		this.start = start;
		this.offset = offset;
	}

	@Override
	public boolean next() throws Exception {
		if (closed) {
			return false;
		}
		if (first) {
			first = false;
			cursor = db.tailMap(prefixKey).entrySet().iterator();
			while (start > 0) {
				if (! cursor.hasNext()) {
					closed = true;
					return false;
				}
				entry = cursor.next();
				if (cmp.compare(prefixKey,
						entry.getKey(), constantBytes) != 0) {
					closed = true;
					return false;
				}
				start--;
			}
		} else if (offset > 0) {
			for (int i = 1; i < offset; i++) {
				if (! cursor.hasNext()) {
					closed = true;
					return false;
				}
				entry = cursor.next();
				if (cmp.compare(prefixKey,
						entry.getKey(), constantBytes) != 0) {
					closed = true;
					return false;
				}
			}
		}
		if (! cursor.hasNext()) {
			closed = true;
			return false;
		}
		entry = cursor.next();
		if (cmp.compare(prefixKey,
				entry.getKey(), constantBytes) != 0) {
			closed = true;
			return false;
		}
		key = entry.getKey();
		value = entry.getValue();
		return true;
	}

	private void getTupleCompressed(Tuple tuple) throws Exception {
		position[0] = 0;
		step.setValue(Utils.decodeInt(value, 0));
		switch (type) {
		case SPO:
			l1.setValue(Utils.decodePackedLong(key, position));
			l2.setValue(Utils.decodePackedLong(key, position));
			l3.setValue(Utils.decodePackedLong(key, position));
			break;
		case SOP:
			l1.setValue(Utils.decodePackedLong(key, position));
			l3.setValue(Utils.decodePackedLong(key, position));
			l2.setValue(Utils.decodePackedLong(key, position));
			break;
		case PSO:
			l2.setValue(Utils.decodePackedLong(key, position));
			l1.setValue(Utils.decodePackedLong(key, position));
			l3.setValue(Utils.decodePackedLong(key, position));
			break;
		case POS:
			l2.setValue(Utils.decodePackedLong(key, position));
			l3.setValue(Utils.decodePackedLong(key, position));
			l1.setValue(Utils.decodePackedLong(key, position));
			break;
		case OSP:
			l3.setValue(Utils.decodePackedLong(key, position));
			l1.setValue(Utils.decodePackedLong(key, position));
			l2.setValue(Utils.decodePackedLong(key, position));
			break;
		case OPS:
			l3.setValue(Utils.decodePackedLong(key, position));
			l2.setValue(Utils.decodePackedLong(key, position));
			l1.setValue(Utils.decodePackedLong(key, position));
			break;
		default:
			// TODO: throw exception?
			l1.setValue(Utils.decodePackedLong(key, position));
			l2.setValue(Utils.decodePackedLong(key, position));
			l3.setValue(Utils.decodePackedLong(key, position));
			break;
		}
		tuple.set(signature);
	}
	
	@Override
	public void getTuple(Tuple tuple) throws Exception {
		if (compressedKeys) {
			getTupleCompressed(tuple);
			return;
		}
		int sOffset, pOffset, oOffset;
		switch (type) {
		case SPO:
			sOffset = 0;
			pOffset = 8;
			oOffset = 16;
			break;
		case SOP:
			sOffset = 0;
			oOffset = 8;
			pOffset = 16;
			break;
		case PSO:
			pOffset = 0;
			sOffset = 8;
			oOffset = 16;
			break;
		case POS:
			pOffset = 0;
			oOffset = 8;
			sOffset = 16;
			break;
		case OSP:
			oOffset = 0;
			sOffset = 8;
			pOffset = 16;
			break;
		case OPS:
			oOffset = 0;
			pOffset = 8;
			sOffset = 16;
			break;
		default:
			// TODO: throw exception?
			sOffset = 0;
			pOffset = 8;
			oOffset = 16;
			break;
		}
		l1.setValue(Utils.decodeLong(key, sOffset));
		l2.setValue(Utils.decodeLong(key, pOffset));
		l3.setValue(Utils.decodeLong(key, oOffset));
		step.setValue(Utils.decodeInt(value, 0));
		tuple.set(signature);
	}

	@Override
	public boolean isReady() {
		return true;
	}

}
