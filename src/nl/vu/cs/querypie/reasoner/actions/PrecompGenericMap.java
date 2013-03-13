package nl.vu.cs.querypie.reasoner.actions;

import java.util.Collection;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class PrecompGenericMap extends Action {

	public static final int RULE_ID = 0;

	private Rule rule;
	private int[] key_positions;
	private int[] positions_to_check;
	private Collection<Long> acceptableValues;

	private SimpleData[] outputTuple;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(RULE_ID, "rule", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// Get the rule

		// FIXME: This code is broken. Currently it executes only one rule
		Rule[] rules = ReasoningContext.getInstance().getRuleset()
				.getAllRulesWithSchemaAndGeneric();
		if (rules == null) {
			return;
		}
		rule = rules[0];

		// Get the positions of the generic patterns that are used in the head
		int[][] shared_vars = rule.getSharedVariablesGen_Head();
		key_positions = new int[shared_vars.length];
		for (int i = 0; i < key_positions.length; ++i) {
			key_positions[i] = shared_vars[i][0];
		}

		// Get the positions in the generic variables that should be checked
		// against the schema
		shared_vars = rule.getSharedVariablesGen_Precomp();
		positions_to_check = new int[shared_vars.length];
		for (int i = 0; i < positions_to_check.length; ++i) {
			positions_to_check[i] = shared_vars[i][0];
		}

		// Get the elements from the precomputed tuples that should be checked
		if (shared_vars.length > 1) {
			throw new Exception("Not implemented yet");
		}
		acceptableValues = rule.getPrecomputedTuples().getSortedSet(
				shared_vars[0][1]);

		// Prepare the tuple to be sent in output
		outputTuple = new TLong[key_positions.length
				+ positions_to_check.length];
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		TLong t = (TLong) tuple.get(positions_to_check[0]);
		if (acceptableValues.contains(t.getValue())) {
			// Forward the triple to the "reduce" phase
			for (int i = 0; i < key_positions.length; ++i) {
				outputTuple[i] = tuple.get(key_positions[i]);
			}
			for (int i = 0; i < positions_to_check.length; ++i) {
				outputTuple[key_positions.length + i] = tuple
						.get(positions_to_check[i]);
			}
			actionOutput.output(outputTuple);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		rule = null;
		acceptableValues = null;
		key_positions = null;
		positions_to_check = null;
		outputTuple = null;
	}
}
