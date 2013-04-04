package nl.vu.cs.querypie.reasoner.actions;

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
	 * filter out unrequired computation. Returns the step after the execution.
	 */
	protected int applyRulesSchemaOnly(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		ActionsHelper.readFakeTuple(actions);
		ParallelExecutionSchemaOnly.addToChain(currentStep - 2, actions);
		ActionsHelper.sort(actions, false);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, false);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		writeDerivations(actions, writeTo, currentStep);
		ActionsHelper.collectToNode(actions);
		ReloadSchema.addToChain(actions, false);
		return currentStep + 1;
	}

	/**
	 * Applies all rules involving generic parts. Uses the currentStep to filter
	 * out unrequired computation. Returns the step after the execution.
	 */
	protected int applyRulesWithGenericPatterns(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		ActionsHelper.readEverythingFromBTree(actions);
		ActionsHelper.reconnectAfter(3, actions);
		GenericRuleExecutor.addToChain(true, currentStep - 2, actions);
		SetStep.addToChain(currentStep, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.mapReduce(actions, currentStep - 2, false);
		SetStep.addToChain(currentStep, actions);
		ActionsHelper.sort(actions, true);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, true);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		writeDerivations(actions, writeTo, currentStep);
		return currentStep + 1;
	}

	protected int applyRulesWithGenericPatternsInABranch(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		ActionSequence actions2 = new ActionSequence();
		currentStep = applyRulesWithGenericPatterns(actions2, writeTo, currentStep);
		ActionsHelper.createBranch(actions, actions2);
		return currentStep;
	}

	private void writeDerivations(ActionSequence actions, MemoryStorage writeTo, int currentStep) throws ActionNotConfiguredException {
		switch (writeTo) {
		case BTREE:
			WriteDerivationsBtree.addToChain(currentStep, actions);
			break;
		case IN_MEMORY:
			WriteInMemory.addToChain(actions, Consts.CURRENT_DELTA_KEY);
			break;
		}
	}

}
