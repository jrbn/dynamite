package nl.vu.cs.dynamite.index;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToBtree extends Action {

	protected static final Logger log = LoggerFactory
			.getLogger(WriteToBtree.class);

	public static final int S_STORAGECLASS = 0;
	public static final int B_COUNT = 1;

	private BTreeInterface in;
	private WritingSession db;
	private final byte[] triple = new byte[24];
	private byte[] value;
	private boolean count, printValue;

	private long countRecords;
	private long countNewRecords;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_STORAGECLASS, "storage class", null, true);
		conf.registerParameter(B_COUNT, "use count", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		in = Class.forName(getParamString(S_STORAGECLASS))
				.asSubclass(BTreeInterface.class).newInstance();
		db = null;
		count = getParamBoolean(B_COUNT);
		if (count) {
			value = new byte[8];
		} else {
			value = new byte[4];
		}
		countNewRecords = countRecords = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (db == null) {
			// Check the type
			TByte type = (TByte) tuple.get(0);
			db = in.openWritingSession(context,
					Partitions.partition_labels[type.getValue()]);
			printValue = Partitions.partition_labels[type.getValue()]
					.equals("spo");
		}

		int keyLen = in.encode(triple, ((TLong) tuple.get(1)).getValue(),
				((TLong) tuple.get(2)).getValue(),
				((TLong) tuple.get(3)).getValue());
		// Write the step
		Utils.encodeInt(value, 0, ((TInt) tuple.get(4)).getValue());

		if (count) {
			if (log.isDebugEnabled() && printValue) {
				log.debug("Write C=" + ((TInt) tuple.get(5)).getValue());
			}
			if (db.writeWithCount(triple, keyLen, value,
					((TInt) tuple.get(5)).getValue(), false) == WritingSession.SUCCESS) {
				countNewRecords++;
			}
		} else {
			if (db.write(triple, keyLen, value) == WritingSession.SUCCESS) {
				countNewRecords++;
			}
		}

		countRecords++;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (db != null) {
			db.close();
		}
		if (in != null) {
			in.close();
			in = null;
		}
		context.incrCounter("Inserts into DB", countRecords);
		context.incrCounter("New Inserts into DB", countNewRecords);
	}
}
