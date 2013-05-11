package nl.vu.cs.dynamite.reasoner.actions.rules;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.dynamite.ReasoningContext;
import nl.vu.cs.dynamite.reasoner.rules.Rule;
import nl.vu.cs.dynamite.storage.Pattern;
import nl.vu.cs.dynamite.storage.Term;
import nl.vu.cs.dynamite.storage.inmemory.Tuples;
import nl.vu.cs.dynamite.storage.inmemory.Tuples.Row;

public class PrecomputedRuleExecutor extends Action {
	public static void addToChain(int minimumStep, int outputStep, int ruleId,
			ActionSequence actions, boolean incrementalFlag)
			throws ActionNotConfiguredException {
		ActionConf a = ActionFactory
				.getActionConf(PrecomputedRuleExecutor.class);
		a.setParamInt(I_RULE_ID, ruleId);
		a.setParamBoolean(B_INCREMENTAL_FLAG, incrementalFlag);
		a.setParamInt(I_MINIMUM_STEP, minimumStep);
		a.setParamInt(I_OUTPUT_STEP, outputStep);
		actions.add(a);
	}

	private Rule rule;
	private int counter;
	private int[][] pos_head_precomp;

	private boolean incrementalFlag;
	private int minStep;
	private int outputStep;

	public static final int I_RULE_ID = 0;
	public static final int B_INCREMENTAL_FLAG = 1;
	public static final int I_MINIMUM_STEP = 2;
	public static final int I_OUTPUT_STEP = 3;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_RULE_ID, "I_RULE_ID", null, true);
		conf.registerParameter(B_INCREMENTAL_FLAG, "B_INCREMENTAL_FLAG", false,
				true);
		conf.registerParameter(I_MINIMUM_STEP, "I_MINIMUM_STEP",
				Integer.MIN_VALUE, true);
		conf.registerParameter(I_OUTPUT_STEP, "I_OUTPUT_STEP",
				Integer.MIN_VALUE, true);
	}

	@Override
	public void startProcess(ActionContext context) {
		int ruleId = getParamInt(I_RULE_ID);
		incrementalFlag = getParamBoolean(B_INCREMENTAL_FLAG);
		minStep = getParamInt(I_MINIMUM_STEP);
		outputStep = getParamInt(I_OUTPUT_STEP);
		rule = ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules().get(ruleId);
		pos_head_precomp = rule.getSharedVariablesHead_Precomp();
		counter = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		Tuples tuples = incrementalFlag ? rule
				.getFlaggedPrecomputedTuples(context) : rule
				.getAllPrecomputedTuples(context);
		SimpleData[] outputTriple = { new TLong(), new TLong(), new TLong(),
				new TInt() };

		// Fill the output with the constants in the head
		Pattern head = rule.getHead();
		for (int i = 0; i < 3; ++i) {
			Term t = head.getTerm(i);
			if (t.getName() == null) {
				((TLong) outputTriple[i]).setValue(t.getValue());
			}
		}
		((TInt) outputTriple[3]).setValue(outputStep);

		for (int i = 0; i < tuples.getNTuples(); ++i) {
			Row r = tuples.getRow(i);
			if (r.getStep() < minStep) {
				continue;
			}
			for (int j = 0; j < pos_head_precomp.length; ++j) {
				((TLong) outputTriple[pos_head_precomp[j][0]]).setValue(r
						.getValue(pos_head_precomp[j][1]).getValue());
			}
			actionOutput.output(outputTriple);
			counter++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		context.incrCounter("derivation-rule-" + rule.getId(), counter);
	}

}
