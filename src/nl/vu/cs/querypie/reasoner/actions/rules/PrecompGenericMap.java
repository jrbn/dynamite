package nl.vu.cs.querypie.reasoner.actions.rules;

import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

public class PrecompGenericMap extends Action {
	public static void addToChain(int minimumStep, boolean incrementalFlag,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(PrecompGenericMap.class);
		c.setParamBoolean(PrecompGenericMap.B_INCREMENTAL_FLAG, incrementalFlag);
		c.setParamInt(PrecompGenericMap.I_MINIMUM_STEP, minimumStep);
		actions.add(c);
	}

	private int[][] key_positions;
	private int[][] positions_to_check;
	private int[][] pos_constants_to_check;
	private long[][] value_constants_to_check;
	private Map<Long, Integer>[] acceptableValues;
	private List<Rule> rules;

	private int minimumStep;

	private final TByteArray oneKey = new TByteArray(new byte[8]);
	private final TByteArray twoKeys = new TByteArray(new byte[16]);
	private final TBoolean valid = new TBoolean();
	private final TByte ruleID = new TByte();
	private final Tuple outputTuple = TupleFactory.newTuple();

	public static final int B_INCREMENTAL_FLAG = 0;
	public static final int I_MINIMUM_STEP = 1;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_INCREMENTAL_FLAG, "incremental flag", false,
				true);
		conf.registerParameter(I_MINIMUM_STEP, "minimum step",
				Integer.MIN_VALUE, true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		boolean incrementalFlag = getParamBoolean(B_INCREMENTAL_FLAG);
		minimumStep = getParamInt(I_MINIMUM_STEP);

		rules = ReasoningContext.getInstance().getRuleset()
				.getAllRulesWithSchemaAndGeneric();

		key_positions = new int[rules.size()][];
		positions_to_check = new int[rules.size()][];
		acceptableValues = new Map[rules.size()];
		pos_constants_to_check = new int[rules.size()][];
		value_constants_to_check = new long[rules.size()][];

		for (int r = 0; r < rules.size(); ++r) {
			Rule rule = rules.get(r);

			// Get the positions of the generic patterns that are used in the
			// head
			int[][] shared_vars = rule.getSharedVariablesGen_Head();
			key_positions[r] = new int[shared_vars.length];
			for (int i = 0; i < key_positions[r].length; ++i) {
				key_positions[r][i] = shared_vars[i][0];
			}

			// Get the positions in the generic variables that should be checked
			// against the schema
			shared_vars = rule.getSharedVariablesGen_Precomp();
			positions_to_check[r] = new int[shared_vars.length];
			for (int i = 0; i < positions_to_check[r].length; ++i) {
				positions_to_check[r][i] = shared_vars[i][0];
			}

			// Get the elements from the precomputed tuples that should be
			// checked
			if (shared_vars.length > 1) {
				throw new Exception("Not implemented yet");
			}

			Tuples acceptableValuesTuples = incrementalFlag ? rule
					.getFlaggedPrecomputedTuples() : rule
					.getAllPrecomputedTuples();
			if (acceptableValuesTuples != null)
				acceptableValues[r] = acceptableValuesTuples
						.getSortedSetWithStep(shared_vars[0][1]);
			pos_constants_to_check[r] = rule
					.getPositionsConstantGenericPattern();
			value_constants_to_check[r] = rule.getValueConstantGenericPattern();
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		for (int r = 0; r < rules.size(); r++) {
			// Does the input match with the generic pattern?
			if (!nl.vu.cs.querypie.reasoner.support.Utils.tupleMatchConstants(
					tuple, pos_constants_to_check[r],
					value_constants_to_check[r])) {
				continue;
			}
			TLong t = (TLong) tuple.get(positions_to_check[r][0]);
			if (acceptableValues[r] != null
					&& acceptableValues[r].containsKey(t.getValue())) {
				// Check whether the step is ok
				int schemaStep = acceptableValues[r].get(t.getValue());
				int currentStep = ((TInt) tuple.get(3)).getValue();
				if (currentStep < minimumStep && schemaStep < minimumStep) {
					continue;
				}
				ruleID.setValue(r);
				valid.setValue(currentStep >= minimumStep);
				if (key_positions[r].length == 1) {
					Utils.encodeLong(oneKey.getArray(), 0,
							((TLong) tuple.get(key_positions[r][0])).getValue());
					outputTuple.set(oneKey, valid, ruleID,
							tuple.get(positions_to_check[r][0]));
				} else { // Two keys
					Utils.encodeLong(twoKeys.getArray(), 0,
							((TLong) tuple.get(key_positions[r][0])).getValue());
					Utils.encodeLong(twoKeys.getArray(), 8,
							((TLong) tuple.get(key_positions[r][1])).getValue());
					outputTuple.set(twoKeys, valid, ruleID,
							tuple.get(positions_to_check[r][0]));
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
