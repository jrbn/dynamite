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
import nl.vu.cs.querypie.reasoner.actions.common.AddDerivationCount;
import nl.vu.cs.querypie.reasoner.actions.common.OneStepRulesControllerToMemory;
import nl.vu.cs.querypie.reasoner.actions.io.RemoveDerivationsBtree;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.reasoner.common.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class IncrRemoveController extends Action {
	public static void addToChain(ActionSequence actions, boolean firstIteration) throws ActionNotConfiguredException {
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
		conf.registerParameter(B_FIRST_ITERATION, "first_iteration", true, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentDelta = new TupleSetImpl();
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
		firstIteration = getParamBoolean(B_FIRST_ITERATION);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (!firstIteration) {
			tuple.copyTo(currentTuple);
			Object completeDeltaObj = context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
			if (completeDeltaObj instanceof TupleSet) {
				TupleSet completeDelta = (TupleSet) completeDeltaObj;
				if (!completeDelta.contains(currentTuple)) {
					currentDelta.add(currentTuple);
					completeDelta.add(currentTuple);
					currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
				}
			} else {
				TupleStepMap completeDelta = (TupleStepMap) completeDeltaObj;
				if (!completeDelta.containsKey(tuple)) {
					currentDelta.add(currentTuple);
				}
				completeDelta.put(currentTuple, 1);
				currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
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
	private void deleteAndReDerive(ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		// No need for re-derivation in case of counting algorithm
		if (ParamHandler.get().isUsingCount()) {
			// Remove the derivations from the B-tree
			ActionsHelper.readFakeTuple(actions);
			RemoveDerivationsBtree.addToChain(actions);
		} else {
			ActionsHelper.collectToNode(actions);
			ActionSequence firstBranch = new ActionSequence();
			ActionSequence secondBranch = new ActionSequence();
			ActionSequence thirdBranch = new ActionSequence();

			// Remove the derivations from the B-tree
			ActionsHelper.readFakeTuple(firstBranch);
			RemoveDerivationsBtree.addToChain(firstBranch);

			// Start one step derivation and write results in memory
			ActionsHelper.readFakeTuple(secondBranch);
			OneStepRulesControllerToMemory.addToChain(secondBranch);

			// Continue deriving by iterating on the IncrAddController
			ActionsHelper.readFakeTuple(thirdBranch);
			IncrAddController.addToChain(thirdBranch, -1, true);

			ActionsHelper.createBranch(secondBranch, thirdBranch);
			ActionsHelper.createBranch(firstBranch, secondBranch);
			ActionsHelper.createBranch(actions, firstBranch);
		}
		actionOutput.branch(actions);
	}

	private void executeOneForwardChainIterationAndRestart(ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		IncrRulesParallelExecution.addToChain(actions);
		ActionsHelper.collectToNode(actions, false);
		if (ParamHandler.get().isUsingCount()) {
			// FIXME: is this required?
			AddDerivationCount.addToChain(actions, false);
		} else {
			ActionsHelper.removeDuplicates(actions);
		}
		IncrRemoveController.addToChain(actions, false);
		actionOutput.branch(actions);
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

}
