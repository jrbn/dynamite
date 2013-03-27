package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.querypie.reasoner.common.Consts;

public abstract class AbstractRulesController extends Action {

	protected void applyRulesSchemaOnly(List<ActionConf> actions,
			boolean writeToBTree, boolean writeToCache) {
		ActionsHelper.runSchemaRulesInParallel(Integer.MIN_VALUE, actions);
		ActionsHelper.runSort(actions, false);
		ActionsHelper.removeDuplicates(actions);
		if (writeToBTree) {
			ActionsHelper.writeDerivationsOnBTree(actions);
		}
		if (writeToCache) {
			ActionsHelper.writeInMemory(actions, Consts.CURRENT_DELTA_KEY);
		}
		ActionsHelper.collectToNode(actions);
		ActionsHelper.reloadSchema(actions, false);
	}

	protected void applyRulesWithGenericPatterns(List<ActionConf> actions,
			boolean writeToBTree, boolean writeToCache) {
		ActionsHelper.readEverythingFromBTree(actions);
		ActionsHelper.reconnectAfter(2, actions);
		ActionsHelper.runGenericRuleExecutor(Integer.MIN_VALUE, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.runMapReduce(actions, Integer.MIN_VALUE, false);
		ActionsHelper.runSort(actions, false);
		ActionsHelper.removeDuplicates(actions);
		if (writeToBTree) {
			ActionsHelper.writeDerivationsOnBTree(actions);
		}
		if (writeToCache) {
			ActionsHelper.writeInMemory(actions, Consts.CURRENT_DELTA_KEY);
		}
	}

	protected void applyRulesWithGenericPatternsInABranch(
			List<ActionConf> actions, boolean writeToBTree, boolean writeToCache) {
		List<ActionConf> actions2 = new ArrayList<ActionConf>();
		applyRulesWithGenericPatterns(actions2, writeToBTree, writeToCache);
		ActionsHelper.createBranch(actions, actions2);
	}

}
