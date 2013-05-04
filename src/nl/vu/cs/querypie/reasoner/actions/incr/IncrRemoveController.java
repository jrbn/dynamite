package nl.vu.cs.querypie.reasoner.actions.incr;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.common.OneStepRulesControllerToMemory;
import nl.vu.cs.querypie.reasoner.actions.io.RemoveDerivationsBtree;
import nl.vu.cs.querypie.reasoner.support.Consts;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class IncrRemoveController extends Action {
	public static void addToChain(boolean firstIteration, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(IncrRemoveController.class);
		c.setParamBoolean(B_FIRST_ITERATION, firstIteration);
		actions.add(c);
	}

	public static final int B_FIRST_ITERATION = 0;

	private boolean firstIteration;
	private TupleSet currentDelta;
	private Tuple currentTuple;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_FIRST_ITERATION, "B_FIRST_ITERATION", true,
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentDelta = new TupleSetImpl();
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
				new TLong());
		firstIteration = getParamBoolean(B_FIRST_ITERATION);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (!firstIteration) {
			tuple.copyTo(currentTuple);
			Object completeDeltaObj = context
					.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
			if (completeDeltaObj instanceof TupleSet) {
				TupleSet completeDelta = (TupleSet) completeDeltaObj;
				if (completeDelta.add(currentTuple)) {
					currentDelta.add(currentTuple);
					currentTuple = TupleFactory.newTuple();
				}
			} else {
				TupleStepMap completeDelta = (TupleStepMap) completeDeltaObj;
				if (!completeDelta.containsKey(tuple)) {
					currentDelta.add(currentTuple);
				}
				completeDelta.put(currentTuple, 1);
				currentTuple = TupleFactory.newTuple();
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (firstIteration) {
			executeOneForwardChainIterationAndRestart(context, actionOutput);
		} else {
			saveCurrentDelta(context);
			if (!currentDelta.isEmpty()) {
				// Repeat the process (execute a new iteration) considering the
				// current delta
				executeOneForwardChainIterationAndRestart(context, actionOutput);
			} else {
				// Move to the second stage of the algorithm
				deleteAndReDerive(context, actionOutput);
			}
		}
	}

	/**
	 * Starts the second stage of the algorithm:
	 * 
	 * 1. Remove information derived from deleted facts
	 * 
	 * 2. Start re-derivation from remaining facts
	 */
	private void deleteAndReDerive(ActionContext context,
			ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		// No need for re-derivation in case of counting algorithm

		ActionsHelper.readFakeTuple(actions);
		RemoveDerivationsBtree.addToChain(actions);

		if (!ParamHandler.get().isUsingCount()) {

			// Re-derive what is possible to derive
			ActionSequence branch = new ActionSequence();
			ActionsHelper.readFakeTuple(branch);
			int step = ParamHandler.get().getLastStep() + 1;
			OneStepRulesControllerToMemory.addToChain(branch, step);

			// The previous action will actually read the entire input. First
			// filter out all the duplicates
			ActionsHelper.sort(branch);
			ActionsHelper.removeDuplicates(branch);
			ActionsHelper.filterStep(branch, step);

			// Collect all the new derivation in one location
			ActionsHelper.collectToNode(false, branch);
			ActionsHelper.writeSchemaTriplesInBtree(actions);
			IncrAddController.addToChain(step + 1, false, branch);
			ActionsHelper.createBranch(actions, branch);
		}
		actionOutput.branch(actions);
	}

	private void executeOneForwardChainIterationAndRestart(
			ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		IncrRulesParallelExecution.addToChain(Integer.MIN_VALUE, actions);
		ActionsHelper.collectToNode(false, actions);
		if (!ParamHandler.get().isUsingCount()) {
			ActionsHelper.removeDuplicates(actions);
		}
		IncrRemoveController.addToChain(false, actions);
		actionOutput.branch(actions);
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

}
