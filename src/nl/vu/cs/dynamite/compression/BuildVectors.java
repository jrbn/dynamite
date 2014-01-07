package nl.vu.cs.dynamite.compression;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.DBType;
import nl.vu.cs.dynamite.storage.StandardTerms;
import nl.vu.cs.dynamite.storage.WritingSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildVectors extends Action {

	static Logger log = LoggerFactory.getLogger(DeconstructTriples.class);

	public static final int I_NPARTITIONS_PER_NODE = 0;

	public static final int S_STORAGECLASS = 1;

	protected TString[] triple = new TString[3];

	protected TString url = new TString();
	protected TLong tripleId = new TLong();
	protected TByte position = new TByte();
	protected TInt hashDomain = new TInt();
	protected TInt hashPath = new TInt();
	protected TInt hashObject = new TInt(0);
	protected long counter;
	private int incr;
	protected Map<String, Long> predefinedTerms = null;

	private Tuple tuple = TupleFactory.newTuple(url, tripleId, position,
			hashDomain, hashPath, hashObject);
	private BTreeInterface in = null;
	private WritingSession t2n = null;
	private int[] decodePosition = new int[1];

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_NPARTITIONS_PER_NODE,
				"I_NPARTITIONS_PER_NODE", 1, false);
		conf.registerParameter(S_STORAGECLASS, "S_STORAGECLASS", null, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// Create an unique counter to reconstruct the triple later on.
		counter = (int) context.getCounter("counter");

		int p = getParamInt(I_NPARTITIONS_PER_NODE);
		incr = context.getNumberNodes() * p;

		predefinedTerms = new HashMap<String, Long>();
		@SuppressWarnings("unchecked")
		Map<String, Long> popularURIs = (Map<String, Long>) context
				.getObjectFromCache("popularURIs");
		if (popularURIs != null) {
			predefinedTerms.putAll(popularURIs);
		}
		predefinedTerms.putAll(StandardTerms.getTextToNumber());
		String storageClass = getParamString(S_STORAGECLASS);
		if (storageClass != null) {
			in = Class.forName(getParamString(S_STORAGECLASS))
					.asSubclass(BTreeInterface.class).newInstance();
			t2n = in.openWritingSession(context, DBType.T2N);
		}
	}

	private void processString(String s) throws Exception {
		if (predefinedTerms.containsKey(s)) {
			url.setValue("#" + predefinedTerms.get(s));
		} else {
			URL urlFormat = new URL(s.substring(1, s.length()-1));
			
			url.setValue(s);
			hashDomain.setValue(urlFormat.getHost().hashCode());
			hashPath.setValue(urlFormat.getPath().hashCode());
			
			if (t2n != null) {
				byte[] key = s.getBytes();
				byte[] value;
				if ((value = t2n.get(key, key.length)) != null) {
					decodePosition[0] = 0;
					long v = Utils.decodePackedLong(value, decodePosition);
					url.setValue("#" + v);
				}
			}
		}
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		// Read the triple
		triple[0] = (TString) inputTuple.get(0);
		triple[1] = (TString) inputTuple.get(1);
		triple[2] = (TString) inputTuple.get(2);

		tripleId.setValue(counter);
		counter += incr;

		//System.out.println(triple[0] + " " + triple[1] + " " + triple[2]);
		
		processString(triple[0].getValue());
		position.setValue(1);
		hashObject.setValue(triple[2].getValue().hashCode());
		output.output(tuple);
		System.out.println(tuple);
		hashObject.setValue(0);

		processString(triple[1].getValue());
		position.setValue(2);
		output.output(tuple);
		System.out.println(tuple);

		processString(triple[2].getValue());
		position.setValue(3);
		output.output(tuple);
		System.out.println(tuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		if (t2n != null) {
			t2n.close();
			t2n = null;
		}
		if (in != null) {
			in.close();
			in = null;
		}
	}
}
