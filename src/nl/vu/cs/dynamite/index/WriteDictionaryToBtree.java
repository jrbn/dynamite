package nl.vu.cs.dynamite.index;

import java.util.Arrays;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.DBType;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteDictionaryToBtree extends Action {

	protected static final Logger log = LoggerFactory
			.getLogger(WriteDictionaryToBtree.class);

	public static final int B_TEXT_NUMBER = 0;

	public static final int S_STORAGECLASS = 1;
	
	private WritingSession db;
	private boolean isTextNumber;
	private long countRecords;
	byte[] cvtBuf = new byte[8];

	private BTreeInterface in;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_TEXT_NUMBER, "B_TEXT_NUMBER", null, true);
		conf.registerParameter(S_STORAGECLASS, "S_STORAGECLASS", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		countRecords = 0;
		isTextNumber = getParamBoolean(B_TEXT_NUMBER);
		in = Class.forName(getParamString(S_STORAGECLASS))
				.asSubclass(BTreeInterface.class).newInstance();
		if (isTextNumber) {
			db = in.openWritingSession(context, DBType.T2N);
		} else {
			db = in.openWritingSession(context, DBType.N2T);
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		byte[] key;
		byte[] value;
		if (isTextNumber) {
			TString text = (TString) tuple.get(0);
			TLong number = (TLong) tuple.get(1);

			key = text.getValue().getBytes();
			int l = Utils.encodePackedLong(cvtBuf, 0, number.getValue());
			value = Arrays.copyOf(cvtBuf, l);
		} else {
			TLong number = (TLong) tuple.get(0);
			TString text = (TString) tuple.get(1);
			int l = Utils.encodePackedLong(cvtBuf, 0, number.getValue());
			key = Arrays.copyOf(cvtBuf, l);
			value = text.getValue().getBytes();
		}
		db.write(key, key.length, value);
		countRecords++;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		db.close();
		in.close();
		context.incrCounter("Dictionary Records insert into DB", countRecords);
	}
}
