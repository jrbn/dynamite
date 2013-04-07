package nl.vu.cs.querypie.reasoner.actions.rules;

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
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.Tuples;
import nl.vu.cs.querypie.storage.inmemory.Tuples.Row;

public class PrecomputedRuleExecutor extends Action {
	public static void addToChain(int minimumStep, int outputStep, int ruleId, ActionSequence actions, boolean incrementalFlag)
			throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
		a.setParamInt(RULE_ID, ruleId);
		a.setParamBoolean(INCREMENTAL_FLAG, incrementalFlag);
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

	public static final int RULE_ID = 0;
	public static final int INCREMENTAL_FLAG = 1;
	public static final int I_MINIMUM_STEP = 2;
	public static final int I_OUTPUT_STEP = 3;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(RULE_ID, "rule", null, true);
		conf.registerParameter(INCREMENTAL_FLAG, "incremental_flag", false, true);
		conf.registerParameter(I_MINIMUM_STEP, "minimum step", Integer.MIN_VALUE, true);
		conf.registerParameter(I_OUTPUT_STEP, "step for the (output) produced tuples", Integer.MIN_VALUE, true);
	}

	@Override
	public void startProcess(ActionContext context) {
		int ruleId = getParamInt(RULE_ID);
		incrementalFlag = getParamBoolean(INCREMENTAL_FLAG);
		minStep = getParamInt(I_MINIMUM_STEP);
		outputStep = getParamInt(I_OUTPUT_STEP);

		rule = ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().get(ruleId);
		rule.reloadPrecomputation(ReasoningContext.getInstance(), context, incrementalFlag, !incrementalFlag);
		pos_head_precomp = rule.getSharedVariablesHead_Precomp();
		counter = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		Tuples tuples = incrementalFlag ? rule.getFlaggedPrecomputedTuples() : rule.getAllPrecomputedTuples();
		SimpleData[] outputTriple = { new TLong(), new TLong(), new TLong(), new TInt() };

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
				((TLong) outputTriple[pos_head_precomp[j][0]]).setValue(r.getValue(pos_head_precomp[j][1]).getValue());
			}
			actionOutput.output(outputTriple);
			counter++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		context.incrCounter("derivation-rule-" + rule.getId(), counter);
	}

}
