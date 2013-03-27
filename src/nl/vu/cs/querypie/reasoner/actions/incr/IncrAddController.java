package nl.vu.cs.querypie.reasoner.actions.incr;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.actions.ActionsHelper;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class IncrAddController extends Action {
	private TupleSet currentDelta;
	private TupleSet completeDelta;
	private Tuple currentTuple;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		completeDelta = (TupleSet) context
				.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		currentDelta = new TupleSetImpl();
		currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
				new TLong());
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

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// In case of new derivations, perform another iteration
		if (!currentDelta.isEmpty()) {
			saveCurrentDelta(context);
			executeAForwardChainingIterationAndRestart(context, actionOutput);
		}
		// Otherwise stop and write complete derivations on btree
		else {
			writeCompleteDeltaToBTree(context, actionOutput);
		}
	}

	private void executeAForwardChainingIterationAndRestart(
			ActionContext context, ActionOutput actionOutput) throws Exception {
		updateAndSaveCompleteDelta(context);
		List<ActionConf> actions = new ArrayList<ActionConf>();
		ActionsHelper.runIncrRulesParallelExecution(actions);
		ActionsHelper.collectToNode(actions);
		ActionsHelper.removeDuplicates(actions);
		ActionsHelper.runIncrAddController(actions);
		actionOutput.branch(actions);
	}

	private void updateAndSaveCompleteDelta(ActionContext context) {
		completeDelta = (TupleSet) context
				.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		completeDelta.addAll(currentDelta);
	}

	private void saveCurrentDelta(ActionContext context) {
		context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
	}

	private void writeCompleteDeltaToBTree(ActionContext context,
			ActionOutput actionOutput) throws Exception {
		updateAndSaveCompleteDelta(context);
		ActionsHelper.writeInMemoryTuplesToBTree(context, actionOutput,
				Consts.COMPLETE_DELTA_KEY);
	}

}
