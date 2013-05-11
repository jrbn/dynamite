package nl.vu.cs.dynamite.storage.berkeleydb;

import java.util.Arrays;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.ajira.utils.Utils.BytesComparator;
import nl.vu.cs.dynamite.storage.DBType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

public class BerkeleydbIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(BerkeleydbIterator.class);

	private static final BytesComparator cmp = new BytesComparator();

	private final Cursor cursor;
	private final DatabaseEntry dbKey;
	private final int constantBytes;
	private final DBType type;

	private boolean first;
	private boolean closed;

	private byte[] key;
	private byte[] value;
	private final DatabaseEntry dbValue;
	private final byte[] prefixKey;
	private final boolean compressedKeys;
	private int start = 0;
	private int offset = 0;
	private final int[] position = new int[1];
	private final SimpleData[] signature;
	private TLong l1, l2, l3;
	private TInt step;

	private SimpleData[] signature4;

	BerkeleydbIterator(DatabaseEntry key, int constantBytes, Database db,
			DBType type, boolean compressedKeys) {
		CursorConfig cc = new CursorConfig();
		cc.setReadUncommitted(true);
		cursor = db.openCursor(null, cc);
		this.dbKey = key;
		this.constantBytes = constantBytes;
		this.type = type;
		this.compressedKeys = compressedKeys;
		first = true;
		closed = false;
		dbValue = new DatabaseEntry();
		prefixKey = Arrays.copyOf(key.getData(), constantBytes);
		signature = new SimpleData[5];
		signature4 = new SimpleData[4];
		// signature = new SimpleData[4];
		signature[0] = signature4[0] = l1 = new TLong();
		signature[1] = signature4[1] = l2 = new TLong();
		signature[2] = signature4[2] = l3 = new TLong();
		signature[3] = signature4[3] = step = new TInt();
		signature[4] = new TInt();
	}

	BerkeleydbIterator(DatabaseEntry key, int constantBytes, Database db,
			DBType dbType, boolean compressedKeys, int start, int offset) {
		this(key, constantBytes, db, dbType, compressedKeys);
		this.start = start;
		this.offset = offset;
	}

	@Override
	public boolean next() throws Exception {
		if (closed)
			return false;
		boolean result = false;
		if (first) {
			OperationStatus status = cursor.getSearchKeyRange(dbKey, dbValue,
					null);
			result = (status == OperationStatus.SUCCESS);
			first = false;
			if (result && start > 0) {
				long skippedValues = cursor.skipNext(start, dbKey, dbValue,
						null);
				result = skippedValues == start;
			}
			key = dbKey.getData();
			// Fix: dbKey was not compared with the prefixKey here. --Ceriel
			result = result
					&& cmp.compare(prefixKey, key, constantBytes) == 0;
		} else {
			if (offset > 0) {
				long skipValues = cursor.skipNext(offset, dbKey, dbValue, null);
				key = dbKey.getData();
				if (skipValues != offset) {
					result = false;
				} else {
					result = cmp.compare(prefixKey, key,
							constantBytes) == 0;
				}
			} else {
				OperationStatus status = cursor.getNext(dbKey, dbValue, null);
				key = dbKey.getData();
				result = (status == OperationStatus.SUCCESS && cmp.compare(
						prefixKey, key, constantBytes) == 0);
			}
		}
		if (!result) {
			cursor.close();
			closed = true;
			return false;
		}
		value = dbValue.getData();
		return result;
	}

	private void getTupleCompressed(Tuple tuple) throws Exception {
		position[0] = 0;
		step.setValue(Utils.decodeInt(value, 0));
		if (value.length > 4) {
			((TInt) signature[4]).setValue(Utils.decodeInt(value, 4));
		}
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
		if (value.length > 4) {
			tuple.set(signature);
		} else {
			tuple.set(signature4);
		}
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
		if (value.length > 4) {
			((TInt) signature[4]).setValue(Utils.decodeInt(value, 4));
			tuple.set(signature);
		} else {
			tuple.set(signature4);
		}

		if (log.isTraceEnabled()) {
			log.trace("Tuple: " + tuple.toString());
		}
	}

	@Override
	public boolean isReady() {
		return true;
	}

}
