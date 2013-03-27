package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.querypie.reasoner.common.Consts;

public abstract class AbstractRulesController extends Action {
	protected void applyRulesSchemaOnly(List<ActionConf> actions, boolean writeToBTree, boolean countDerivations, int step, boolean flaggedOnly) {
		ActionsHelper.runSchemaRulesInParallel(step - 3, actions);
		ActionsHelper.runSort(actions, false);
		if (countDerivations) {
			ActionsHelper.addDerivationCount(actions, false);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		if (writeToBTree) {
			ActionsHelper.writeDerivationsOnBTree(true, step, actions);
		} else {
			ActionsHelper.writeInMemory(actions, Consts.CURRENT_DELTA_KEY);
		}
		ActionsHelper.collectToNode(actions);
		ActionsHelper.reloadSchema(actions, false);
	}

	protected void applyRulesWithGenericPatterns(List<ActionConf> actions, boolean writeToBTree, boolean countDerivations, int step, boolean flaggedOnly) {
		ActionsHelper.readEverythingFromBTree(actions);
		ActionsHelper.reconnectAfter(3, actions);
		ActionsHelper.runGenericRuleExecutor(step - 3, actions);
		ActionsHelper.setStep(step, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.runMapReduce(actions, step - 2, false);
		ActionsHelper.runSort(actions, true);
		if (countDerivations) {
			ActionsHelper.addDerivationCount(actions, true);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		if (writeToBTree) {
			ActionsHelper.writeDerivationsOnBTree(false, step, actions);
		} else {
			ActionsHelper.writeInMemory(actions, Consts.CURRENT_DELTA_KEY);
		}
	}

	protected void applyRulesWithGenericPatternsInABranch(List<ActionConf> actions, boolean writeToBTree, boolean countDerivations, int step,
			boolean flaggedOnly) {
		List<ActionConf> actions2 = new ArrayList<ActionConf>();
		applyRulesWithGenericPatterns(actions2, writeToBTree, countDerivations, step, flaggedOnly);
		ActionsHelper.createBranch(actions, actions2);
	}

}
