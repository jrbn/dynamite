package nl.vu.cs.querypie.reasoner.actions;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ParallelExecutionSchemaOnly extends Action {
	public static void addToChain(int step, List<ActionConf> actions) {
		ActionConf a = ActionFactory.getActionConf(ParallelExecutionSchemaOnly.class);
		a.setParamInt(ParallelExecutionSchemaOnly.I_STEP, step);
		actions.add(a);
	}

	public static final int I_STEP = 0;

	private int step;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", Integer.MIN_VALUE, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		step = getParamInt(I_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		List<Rule> rules = ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules();
		ActionsHelper.parallelRunPrecomputedRuleExecutorForAllRules(step, rules.size(), false, actionOutput);
	}
}
