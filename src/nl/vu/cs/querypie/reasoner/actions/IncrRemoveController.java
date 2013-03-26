package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTreeTupleSet;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;

public class IncrRemoveController extends Action {
  private InMemoryTupleSet currentDelta;
  private InMemoryTupleSet completeDelta;
  private Tuple currentTuple;

  @Override
  public void startProcess(ActionContext context) throws Exception {
    currentDelta = new InMemoryTreeTupleSet();
    completeDelta = (InMemoryTupleSet) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
    currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    tuple.copyTo(currentTuple);
    if (!completeDelta.contains(currentTuple)) {
      currentDelta.add(currentTuple);
      currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
    }
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    saveCurrentDelta(context);
    if (!currentDelta.isEmpty()) {
      // Repeat the process (execute a new iteration) considering the current delta
      executeOneForwardChainIterationAndRestartFromStage(context, actionOutput);
    } else {
      // Move to the second stage of the algorithm.
      List<ActionConf> actions = new ArrayList<ActionConf>();
      List<ActionConf> actionsToBranch = new ArrayList<ActionConf>();
      removeAllInMemoryTuplesFromBTree(context);
      ActionsHelper.runOneStepRulesControllerToMemory(actions);
      ActionsHelper.readAllInMemoryTuples(actionsToBranch, Consts.COMPLETE_DELTA_KEY);
      ActionsHelper.runIncrAddController(actionsToBranch);
      ActionsHelper.createBranch(actions, actionsToBranch);
      actionOutput.branch(actions);
    }
  }

  private void executeOneForwardChainIterationAndRestartFromStage(ActionContext context, ActionOutput actionOutput) throws Exception {
    updateAndSaveCompleteDelta(context);
    List<ActionConf> actions = new ArrayList<ActionConf>();
    ActionsHelper.runIncrRulesParallelExecution(actions);
    ActionsHelper.collectToNode(actions);
    ActionsHelper.removeDuplicates(actions);
    ActionsHelper.runIncrRemoveController(actions);
    actionOutput.branch(actions);
  }

  private void updateAndSaveCompleteDelta(ActionContext context) {
    completeDelta = (InMemoryTupleSet) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
    completeDelta.addAll(currentDelta);
    context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
  }

  private void saveCurrentDelta(ActionContext context) {
    context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
  }

  private void removeAllInMemoryTuplesFromBTree(ActionContext context) {
    InMemoryTupleSet set = (InMemoryTupleSet) context.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
    BerkeleydbLayer db = ReasoningContext.getInstance().getKB();
    for (Tuple t : set) {
      db.remove(t);
    }
  }
}
