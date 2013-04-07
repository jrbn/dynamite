package nl.vu.cs.querypie.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.actions.io.MemoryStorage;
import nl.vu.cs.querypie.reasoner.actions.io.WriteDerivationsBtree;
import nl.vu.cs.querypie.reasoner.actions.io.WriteInMemory;
import nl.vu.cs.querypie.reasoner.actions.rules.GenericRuleExecutor;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;

public abstract class AbstractRulesController extends Action {

	/**
	 * Applies all rules involving only the schema. Uses the currentStep to
	 * filter out unneeded computation. Returns the step after the execution.
	 */
	protected int applyRulesSchemaOnly(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		ActionsHelper.readFakeTuple(actions);
		ParallelExecutionSchemaOnly.addToChain(currentStep - 2, currentStep, actions);
		ActionsHelper.sort(actions);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		writeDerivations(writeTo, actions);
		ActionsHelper.collectToNode(actions);
		ReloadSchema.addToChain(false, actions);
		return currentStep + 1;
	}

	/**
	 * Applies all rules involving generic parts. Uses the currentStep to filter
	 * out unneeded computation. Returns the step after the execution.
	 */
	protected int applyRulesWithGenericPatterns(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		ActionsHelper.readEverythingFromBTree(actions);
		ActionsHelper.reconnectAfter(2, actions);
		GenericRuleExecutor.addToChain(currentStep - 2, currentStep, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.mapReduce(currentStep - 2, currentStep, false, actions);
		ActionsHelper.sort(actions);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		writeDerivations(writeTo, actions);
		return currentStep + 1;
	}

	protected int applyRulesWithGenericPatternsInABranch(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		ActionSequence actions2 = new ActionSequence();
		currentStep = applyRulesWithGenericPatterns(actions2, writeTo, currentStep);
		ActionsHelper.createBranch(actions, actions2);
		return currentStep;
	}

	private void writeDerivations(MemoryStorage writeTo, ActionSequence actions) throws ActionNotConfiguredException {
		switch (writeTo) {
		case BTREE:
			WriteDerivationsBtree.addToChain(actions);
			break;
		case IN_MEMORY:
			WriteInMemory.addToChain(Consts.CURRENT_DELTA_KEY, actions);
			break;
		}
	}

}
