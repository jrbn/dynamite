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
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.common.OneStepRulesControllerToMemory;
import nl.vu.cs.querypie.reasoner.actions.io.RemoveDerivationsBtree;
import nl.vu.cs.querypie.reasoner.support.Consts;
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class IncrRemoveController extends Action {
	public static void addToChain(boolean firstIteration, boolean countingAlgo,
			ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(IncrRemoveController.class);
		c.setParamBoolean(B_FIRST_ITERATION, firstIteration);
		c.setParamBoolean(B_IS_COUNTING, countingAlgo);
		actions.add(c);
	}

	public static final int B_FIRST_ITERATION = 0;
	public static final int B_IS_COUNTING = 1;

	private boolean firstIteration;
	private boolean countingAlgo;

	private TupleSet currentDelta;
	private Tuple currentTuple;

	@Override
	protected void registerActionParameters(ActionConf conf) {
		conf.registerParameter(B_FIRST_ITERATION, "First iteration", true,
				false);
		conf.registerParameter(B_IS_COUNTING, "Is counting", true, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentDelta = new TupleSetImpl();
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
				new TLong());
		firstIteration = getParamBoolean(B_FIRST_ITERATION);
		countingAlgo = getParamBoolean(B_IS_COUNTING);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (!firstIteration) {
			TupleSet completeDelta = (TupleSet) context
					.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
			tuple.copyTo(currentTuple);
			if (!countingAlgo) {
				if (completeDelta.add(currentTuple)) {
					currentDelta.add(currentTuple);
					currentTuple = TupleFactory.newTuple();
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Processing tuple " + tuple);
				}
				if (!completeDelta.contains(tuple)) {
					if (log.isDebugEnabled()) {
						log.debug("Not in completeDelta");
					}
					if (log.isDebugEnabled()) {
						log.debug("Current count is "
								+ ReasoningContext.getInstance().getDBHandler()
										.getCount(context, currentTuple));
					}
					if (ReasoningContext
							.getInstance()
							.getDBHandler()
							.decreaseAndRemoveTriple(context, currentTuple, 1 /* FIXME */)) {
						if (log.isDebugEnabled()) {
							log.debug("Removed from database!");
						}
						currentDelta.add(currentTuple);
						completeDelta.add(currentTuple);
						currentTuple = TupleFactory.newTuple();
					}
				}
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
		// No need for re-derivation in case of counting algorithm
		if (ParamHandler.get().isUsingCount()) {
			return;
		}

		ActionSequence actions = new ActionSequence();

		ActionsHelper.readFakeTuple(actions);
		RemoveDerivationsBtree.addToChain(actions);

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

		// ActionsHelper.writeSchemaTriplesInBtree(branch);

		IncrAddController.addToChain(step + 1, false, branch);
		ActionsHelper.createBranch(actions, branch);
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
		IncrRemoveController.addToChain(false, countingAlgo, actions);
		actionOutput.branch(actions);
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

}
