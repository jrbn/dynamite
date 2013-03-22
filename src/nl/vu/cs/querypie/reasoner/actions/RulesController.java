package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesController extends Action {

	public static final int I_STEP = 0;
	static final Logger log = LoggerFactory.getLogger(RulesController.class);

	private boolean hasDerived;
	private int step;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_STEP, "step", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		hasDerived = false;
		step = getParamInt(I_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		hasDerived = true;
	}

	private void applyRulesWithGenericPatterns(List<ActionConf> actions) {
		ActionsHelper.readEverythingFromBTree(actions);
		ActionsHelper.reconnectAfter(2, actions);
		ActionsHelper.runGenericRuleExecutor(step - 1, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.runMapReduce(actions, false);
		ActionsHelper.runSort(actions);
		ActionsHelper.runRemoveDuplicates(actions);
		ActionsHelper.runWriteDerivationsOnBTree(step, actions);
	}

	private void applyRulesSchemaOnly(List<ActionConf> actions) {
		ActionsHelper.readFakeTuple(actions);
		ActionsHelper.runSchemaRulesInParallel(actions);
		ActionsHelper.runSort(actions);
		ActionsHelper.runRemoveDuplicates(actions);
		ActionsHelper.runWriteDerivationsOnBTree(step, actions);
		ActionsHelper.runReloadSchema(actions, false);
	}

	private void applyRulesWithGenericPatternsInABranch(List<ActionConf> actions) {
		List<ActionConf> actions2 = new ArrayList<ActionConf>();
		applyRulesWithGenericPatterns(actions2);
		ActionsHelper.createBranch(actions, actions2);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (!hasDerived)
			return;
		List<ActionConf> actions = new ArrayList<ActionConf>();
		if (!ReasoningContext.getInstance().getRuleset()
				.getAllSchemaOnlyRules().isEmpty()) {
			applyRulesSchemaOnly(actions);
			applyRulesWithGenericPatternsInABranch(actions);
		} else {
			applyRulesWithGenericPatterns(actions);
		}
		ActionsHelper.runCollectToNode(actions);
		ActionsHelper.runRulesController(step + 1, actions);
		actionOutput.branch(actions);
	}

}
