package nl.vu.cs.querypie.reasoner.actions;

import java.util.Collection;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class PrecompGenericMap extends Action {

	private int[][] key_positions;
	private int[][] positions_to_check;
	private Collection<Long>[] acceptableValues;
	private Rule[] rules;

	private final TByteArray oneKey = new TByteArray(new byte[9]);
	private final TByteArray twoKeys = new TByteArray(new byte[17]);
	private final Tuple outputTuple = TupleFactory.newTuple();

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {

		rules = ReasoningContext.getInstance().getRuleset()
				.getAllRulesWithSchemaAndGeneric();
		key_positions = new int[rules.length][];
		positions_to_check = new int[rules.length][];
		acceptableValues = new Collection[rules.length];

		for (int m = 0; m < rules.length; ++m) {
			Rule rule = rules[m];

			// Get the positions of the generic patterns that are used in the
			// head
			int[][] shared_vars = rule.getSharedVariablesGen_Head();
			key_positions[m] = new int[shared_vars.length];
			for (int i = 0; i < key_positions[m].length; ++i) {
				key_positions[m][i] = shared_vars[i][0];
			}

			// Get the positions in the generic variables that should be checked
			// against the schema
			shared_vars = rule.getSharedVariablesGen_Precomp();
			positions_to_check[m] = new int[shared_vars.length];
			for (int i = 0; i < positions_to_check[m].length; ++i) {
				positions_to_check[m][i] = shared_vars[i][0];
			}

			// Get the elements from the precomputed tuples that should be
			// checked
			if (shared_vars.length > 1) {
				throw new Exception("Not implemented yet");
			}

			acceptableValues[m] = rule.getPrecomputedTuples().getSortedSet(
					shared_vars[0][1]);

		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		for (int m = 0; m < rules.length; m++) {
			TLong t = (TLong) tuple.get(positions_to_check[m][0]);
			if (acceptableValues[m].contains(t.getValue())) {

				if (key_positions[m].length == 1) {
					oneKey.getArray()[0] = (byte) m;
					Utils.encodeLong(oneKey.getArray(), 1,
							((TLong) tuple.get(key_positions[m][0])).getValue());
					outputTuple
							.set(oneKey, tuple.get(positions_to_check[m][0]));

				} else { // Two keys
					twoKeys.getArray()[0] = (byte) m;
					Utils.encodeLong(twoKeys.getArray(), 1,
							((TLong) tuple.get(key_positions[m][0])).getValue());
					Utils.encodeLong(twoKeys.getArray(), 9,
							((TLong) tuple.get(key_positions[m][1])).getValue());
					outputTuple.set(twoKeys,
							tuple.get(positions_to_check[m][0]));
				}
				actionOutput.output(outputTuple);
			}
		}

	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		rules = null;
		acceptableValues = null;
		key_positions = null;
		positions_to_check = null;
	}
}
