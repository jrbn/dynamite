package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class RuleExecutor1 extends Action {

	public static final int RULE_ID = 0;

	private Rule rule;
	private int[] key_positions;
	private int[] positions_to_check;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(RULE_ID, "rule", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// Get the rule
		rule = ReasoningContext.getInstance().getRule(getParamInt(RULE_ID));

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

	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		rule = null;
	}
}
