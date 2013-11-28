package nl.vu.cs.dynamite.decompression;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.dynamite.storage.StandardTerms;

public class DeconstructTriples extends Action {

	private final TLong[] triple = { new TLong(), new TLong(), new TLong() };
	private final TLong tripleId = new TLong();
	private final Tuple outputTuple = TupleFactory.newTuple();
	private long tripleCounter = 0;
	private long statsTriples, statsDict;

	private static final TByte DICT = new TByte(0);
	private static final TByte TRIPLE = new TByte(1);
	private static final TByte ALREADY_CONVERTED = new TByte(2);
	private static final TByte SUBJ = new TByte(0);
	private static final TByte PRED = new TByte(1);
	private static final TByte OBJ = new TByte(2);
	private static final TLong NONE = new TLong(-1);
	private static final TString NULL = new TString(null);

	private TString txtValue;
	private Map<Long, String> commonValues;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		int c = (int) context.getCounter("tripleCounter") + 1;
		if (c > 0xFFFF) {
			// Takes more than 2 bytes to store. This case is not supported.
			throw new Exception("Not supported");
		}
		tripleCounter = (long) c << 48;

		commonValues = new HashMap<Long, String>();
		@SuppressWarnings("unchecked")
		Map<Long, String> popularURIs = (Map<Long, String>) context
				.getObjectFromCache("popularTextURIs");
		if (popularURIs != null) {
			commonValues.putAll(popularURIs);
		}
		commonValues.putAll(StandardTerms.getNumberToText());

		statsTriples = statsDict = 0;
		txtValue = new TString(null);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		// Triple
		if (inputTuple.getNElements() == 3) {

			statsTriples++;

			triple[0] = (TLong) inputTuple.get(0);
			triple[1] = (TLong) inputTuple.get(1);
			triple[2] = (TLong) inputTuple.get(2);

			tripleId.setValue(tripleCounter++);

			if (commonValues.containsKey(triple[0].getValue())) {
				txtValue.setValue(commonValues.get(triple[0].getValue()));
				outputTuple.set(tripleId, ALREADY_CONVERTED, SUBJ, NONE,
						txtValue);
			} else {
				outputTuple.set(triple[0], TRIPLE, SUBJ, tripleId, NULL);
			}
			output.output(outputTuple);

			if (commonValues.containsKey(triple[1].getValue())) {
				txtValue.setValue(commonValues.get(triple[1].getValue()));
				outputTuple.set(tripleId, ALREADY_CONVERTED, PRED, NONE,
						txtValue);
			} else {
				outputTuple.set(triple[1], TRIPLE, PRED, tripleId, NULL);
			}
			output.output(outputTuple);

			if (commonValues.containsKey(triple[2].getValue())) {
				txtValue.setValue(commonValues.get(triple[2].getValue()));
				outputTuple.set(tripleId, ALREADY_CONVERTED, OBJ, NONE,
						txtValue);
			} else {
				outputTuple.set(triple[2], TRIPLE, OBJ, tripleId, NULL);
			}
			output.output(outputTuple);

		} else { // Dictionary
			triple[0] = (TLong) inputTuple.get(0);
			txtValue = (TString) inputTuple.get(1);
			statsDict++;
			if (!commonValues.containsKey(triple[0].getValue())) {
				outputTuple.set(triple[0], DICT, OBJ, NONE, txtValue);
				output.output(outputTuple);
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		context.incrCounter("input triples", statsTriples);
		context.incrCounter("input dictionary entries", statsDict);
	}

}
