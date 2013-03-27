package nl.vu.cs.querypie.reasoner.actions.incr;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.OneStepRulesControllerFromMemory;
import nl.vu.cs.querypie.reasoner.actions.io.ReadAllInMemoryTriples;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class IncrRemoveController extends Action {

	public static void addToChain(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(IncrRemoveController.class);
		actions.add(c);
	}

	private TupleSet currentDelta;

	private TupleSet completeDelta;
	private Tuple currentTuple;

	private void executeOneForwardChainIterationAndRestartFromStage(
			ActionContext context, ActionOutput actionOutput) throws Exception {
		updateAndSaveCompleteDelta(context);
		List<ActionConf> actions = new ArrayList<ActionConf>();
		IncrRulesParallelExecution.addToChain(actions);
		ActionsHelper.collectToNode(actions);
		ActionsHelper.removeDuplicates(actions);
		IncrRemoveController.addToChain(actions);
		actionOutput.branch((ActionConf[]) actions.toArray());
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		tuple.copyTo(currentTuple);
		if (!completeDelta.contains(currentTuple)) {
			currentDelta.add(currentTuple);
			currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
					new TLong());
		}
	}

	private void removeAllInMemoryTuplesFromBTree(ActionContext context) {
		TupleSet set = (TupleSet) context
				.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
		BerkeleydbLayer db = ReasoningContext.getInstance().getKB();
		for (Tuple t : set) {
			db.remove(t);
		}
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		currentDelta = new TupleSetImpl();
		completeDelta = (TupleSetImpl) context
				.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
				new TLong());
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		saveCurrentDelta(context);
		if (!currentDelta.isEmpty()) {
			// Repeat the process (execute a new iteration) considering the
			// current delta
			executeOneForwardChainIterationAndRestartFromStage(context,
					actionOutput);
		} else {
			// Move to the second stage of the algorithm.
			List<ActionConf> actions = new ArrayList<ActionConf>();
			List<ActionConf> actionsToBranch = new ArrayList<ActionConf>();
			removeAllInMemoryTuplesFromBTree(context);
			OneStepRulesControllerFromMemory.addToChain(actions);
			ActionsHelper.collectToNode(actions);

			ActionsHelper.readFakeTuple(actionsToBranch);
			ReadAllInMemoryTriples.addToChain(actionsToBranch,
					Consts.COMPLETE_DELTA_KEY);
			IncrAddController.addToChain(actionsToBranch, -1);
			ActionsHelper.createBranch(actions, actionsToBranch);

			actionOutput.branch((ActionConf[]) actions.toArray());
		}
	}

	private void updateAndSaveCompleteDelta(ActionContext context) {
		completeDelta = (TupleSet) context
				.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		completeDelta.addAll(currentDelta);
		context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
	}
}
