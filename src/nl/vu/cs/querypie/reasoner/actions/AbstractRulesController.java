package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.actions.io.WriteDerivationsBtree;
import nl.vu.cs.querypie.reasoner.actions.io.WriteInMemory;
import nl.vu.cs.querypie.reasoner.actions.rules.GenericRuleExecutor;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;

public abstract class AbstractRulesController extends Action {
	protected void applyRulesSchemaOnly(ActionSequence actions,
			boolean writeToBTree, int step, boolean flaggedOnly)
			throws ActionNotConfiguredException {
		ActionsHelper.readFakeTuple(actions);
		ParallelExecutionSchemaOnly.addToChain(step - 3, actions);
		ActionsHelper.sort(actions, false);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, false);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		if (writeToBTree) {
			WriteDerivationsBtree.addToChain(true, step, actions);
		} else {
			WriteInMemory.addToChain(actions, Consts.CURRENT_DELTA_KEY);
		}
		ActionsHelper.collectToNode(actions);
		ReloadSchema.addToChain(actions, false);
	}

	protected void applyRulesWithGenericPatterns(ActionSequence actions,
			boolean writeToBTree, int step, boolean flaggedOnly)
			throws ActionNotConfiguredException {
		ActionsHelper.readEverythingFromBTree(actions);
		ActionsHelper.reconnectAfter(3, actions);
		GenericRuleExecutor.addToChain(true, step, actions);
		SetStep.addToChain(step, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.mapReduce(actions, step - 2, false);
		SetStep.addToChain(step + 1, actions);
		ActionsHelper.sort(actions, true);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, true);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		if (writeToBTree) {
			WriteDerivationsBtree.addToChain(false, step, actions);
		} else {
			WriteInMemory.addToChain(actions, Consts.CURRENT_DELTA_KEY);
		}
	}

	protected void applyRulesWithGenericPatternsInABranch(
			ActionSequence actions, boolean writeToBTree, int step,
			boolean flaggedOnly) throws ActionNotConfiguredException {
		ActionSequence actions2 = new ActionSequence();
		applyRulesWithGenericPatterns(actions2, writeToBTree, step, flaggedOnly);
		ActionsHelper.createBranch(actions, actions2);
	}

}
