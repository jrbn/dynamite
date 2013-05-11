package nl.vu.cs.dynamite.reasoner.actions.common;

import java.io.File;
import java.io.FilenameFilter;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.dynamite.reasoner.actions.io.TypeStorage;
import nl.vu.cs.dynamite.reasoner.actions.io.WriteDerivationsAllBtree;
import nl.vu.cs.dynamite.reasoner.actions.io.WriteInMemory;
import nl.vu.cs.dynamite.reasoner.actions.rules.GenericRuleExecutor;
import nl.vu.cs.dynamite.reasoner.support.Consts;
import nl.vu.cs.dynamite.reasoner.support.ParamHandler;

public abstract class AbstractRulesController extends Action {

	public static class FilterCountFiles implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			return !name.startsWith("_count");
		}

	}

	/**
	 * Applies all rules involving only the schema. Uses the currentStep to
	 * filter out unneeded computation. Returns the step after the execution.
	 */
	protected int applyRulesSchemaOnly(ActionSequence actions,
			TypeStorage writeTo, int currentStep)
			throws ActionNotConfiguredException {
		ActionsHelper.readFakeTuple(actions);
		ParallelExecutionSchemaOnly.addToChain(currentStep - 3, currentStep,
				actions);
		ActionsHelper.sort(actions);
		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, currentStep);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}

		writeDerivations(writeTo, actions);
		if (writeTo == TypeStorage.BTREE) {
			ActionsHelper.writeCopyToFiles(ParamHandler.get().getCopyDir(),
					actions, false);
		}

		// Forward only the first
		ActionsHelper.forwardOnlyFirst(actions);

		// Get results on one node
		ActionsHelper.collectToNode(ParamHandler.get().isUsingCount(), actions);

		// Invalidate precomputation
		ActionsHelper.reloadPrecomputationForSchemaGenericRules(actions);

		return currentStep + 1;
	}

	/**
	 * Applies all rules involving generic parts. Uses the currentStep to filter
	 * out unneeded computation. Returns the step after the execution.
	 */
	protected int applyRulesWithGenericPatterns(ActionSequence actions,
			TypeStorage writeTo, int currentStep)
			throws ActionNotConfiguredException {
		ActionsHelper.readEverythingFromFiles(ParamHandler.get().getCopyDir(),
				actions, FilterCountFiles.class);

		ActionsHelper.filterPotentialInput(7, actions);

		// Remove the rules
		ActionsHelper.reconnectAfter(2, actions);
		GenericRuleExecutor.addToChain(currentStep - 3, currentStep, actions);
		ActionsHelper.reconnectAfter(4, actions);
		ActionsHelper.mapReduce(currentStep - 2, currentStep + 1, false,
				actions);

		// Sort and maintain only the new derivations
		ActionsHelper.sort(actions);

		if (ParamHandler.get().isUsingCount()) {
			AddDerivationCount.addToChain(actions, currentStep);
			// Write the "counts" in special files
			ActionsHelper.writeCopyToFiles(ParamHandler.get().getCopyDir(),
					actions, true);
		} else {
			ActionsHelper.removeDuplicates(actions);
			ActionsHelper.filterStep(actions, currentStep);
		}

		if (writeTo == TypeStorage.IN_MEMORY) {
			writeDerivations(writeTo, actions);
		} else {
			ActionsHelper.writeCopyToFiles(ParamHandler.get().getCopyDir(),
					actions, false);
			ActionsHelper.writeSchemaTriplesInBtree(actions);
		}

		// Forward only the first
		ActionsHelper.forwardOnlyFirst(actions);

		return currentStep + 2;
	}

	protected int applyRulesWithGenericPatternsInABranch(
			ActionSequence actions, TypeStorage writeTo, int currentStep)
			throws ActionNotConfiguredException {
		ActionSequence actions2 = new ActionSequence();
		currentStep = applyRulesWithGenericPatterns(actions2, writeTo,
				currentStep);
		ActionsHelper.createBranch(actions, actions2);
		return currentStep;
	}

	private void writeDerivations(TypeStorage writeTo, ActionSequence actions)
			throws ActionNotConfiguredException {
		switch (writeTo) {
		case BTREE:
			WriteDerivationsAllBtree.addToChain(actions);
			break;
		case IN_MEMORY:
			WriteInMemory.addToChain(Consts.CURRENT_DELTA_KEY, actions);
			break;
		}
	}

}
