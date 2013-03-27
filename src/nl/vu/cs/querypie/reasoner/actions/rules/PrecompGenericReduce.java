package nl.vu.cs.querypie.reasoner.actions.rules;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.Tuples;
import nl.vu.cs.querypie.storage.inmemory.Tuples.Row;

public class PrecompGenericReduce extends Action {

	public static final int INCREMENTAL_FLAG = 0;
	public static final int I_MINIMUM_STEP = 1;

	private int[][][] pos_head_precomps;
	private int[][][] pos_gen_precomps;
	private int[][][] pos_gen_head;
	private Tuples[] precompTuples;

	private TLong[][] outputTuples;
	private int[] counters;
	private List<Rule> rules;
	private final Set<Tuple> duplicates = new HashSet<Tuple>();
	private Tuple supportTuple = TupleFactory.newTuple();
	private int minimumStep;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(INCREMENTAL_FLAG, "incremental_flag", false,
				false);
		conf.registerParameter(I_MINIMUM_STEP, "minimum step",
				Integer.MIN_VALUE, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		boolean incrementalFlag = getParamBoolean(INCREMENTAL_FLAG);
		minimumStep = getParamInt(I_MINIMUM_STEP);
		rules = ReasoningContext.getInstance().getRuleset()
				.getAllRulesWithSchemaAndGeneric();
		counters = new int[rules.size()];
		pos_head_precomps = new int[rules.size()][][];
		pos_gen_precomps = new int[rules.size()][][];
		pos_gen_head = new int[rules.size()][][];
		precompTuples = new Tuples[rules.size()];
		outputTuples = new TLong[rules.size()][];

		for (int r = 0; r < rules.size(); ++r) {
			Rule rule = rules.get(r);
			pos_head_precomps[r] = rule.getSharedVariablesHead_Precomp();
			pos_gen_precomps[r] = rule.getSharedVariablesGen_Precomp();
			pos_gen_head[r] = rule.getSharedVariablesGen_Head();
			precompTuples[r] = incrementalFlag ? rule
					.getFlaggedPrecomputedTuples() : rule
					.getAllPrecomputedTuples();
			if (pos_gen_precomps[r].length > 1) {
				throw new Exception("Not supported");
			}

			// Fill the outputTriple with the constants that come from the head
			// of the rule
			Pattern head = rule.getHead();
			outputTuples[r] = new TLong[3];
			for (int i = 0; i < 3; ++i) {
				Term t = head.getTerm(i);
				if (t.getName() == null) {
					outputTuples[r][i] = new TLong(t.getValue());
				} else {
					outputTuples[r][i] = new TLong();
				}
			}
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		duplicates.clear();
		TByteArray key = (TByteArray) tuple.get(0);

		// Perform the join between the "value" part of the triple and the
		// precomputed tuples. Notice that this works only if there is one
		// element to join.
		TBag values = (TBag) tuple.get(tuple.getNElements() - 1);

		int currentRule = -1;
		long currentJoinValue = -1;
		for (Tuple t : values) {
			TBoolean valid = (TBoolean) t.get(0);
			TByte rule = (TByte) t.get(1);
			TLong elementToJoin = (TLong) t.get(2);

			// if (rule.getValue() == currentRule
			// && currentJoinValue == elementToJoin.getValue()) {
			// continue;
			// } else {

			if (currentRule != rule.getValue()) {
				currentRule = rule.getValue();
				// First copy the "key" in the output triple.
				for (int i = 0; i < pos_gen_head[currentRule].length; ++i) {
					outputTuples[currentRule][pos_gen_head[currentRule][i][1]]
							.setValue(Utils.decodeLong(key.getArray(), 8 * i));
				}
			}
			// }

			currentJoinValue = elementToJoin.getValue();
			Collection<Row> set = precompTuples[currentRule].get(
					pos_gen_precomps[currentRule][0][1], currentJoinValue);
			if (set != null) {
				for (Row row : set) {

					// Only if the step is ok
					if (!valid.getValue() && row.getStep() < minimumStep) {
						continue;
					}

					// Get current values
					for (int i = 0; i < pos_head_precomps[currentRule].length; ++i) {
						outputTuples[currentRule][pos_head_precomps[currentRule][i][0]]
								.setValue(row.getValue(
										pos_head_precomps[currentRule][i][1])
										.getValue());
					}
					supportTuple.set(outputTuples[currentRule]);
					if (!duplicates.contains(supportTuple)) {
						duplicates.add(supportTuple);
						supportTuple = TupleFactory.newTuple();
						actionOutput.output(outputTuples[currentRule]);
						counters[currentRule]++;
					}
				}
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		pos_head_precomps = null;
		pos_gen_precomps = null;
		pos_gen_head = null;
		precompTuples = null;
		outputTuples = null;
		for (int i = 0; i < counters.length; ++i) {
			if (counters[i] > 0) {
				context.incrCounter("derivation-rule-" + rules.get(i).getId(),
						counters[i]);
			}
		}
	}
}
