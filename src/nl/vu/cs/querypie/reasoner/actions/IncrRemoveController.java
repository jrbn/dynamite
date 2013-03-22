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
  static final int I_STAGE = 0;
  private int stage = 0;

  private InMemoryTupleSet currentDelta;
  private InMemoryTupleSet completeDelta;
  private Tuple currentTuple;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(I_STAGE, "stage computation", 0, false);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    stage = getParamInt(I_STAGE);
    currentDelta = new InMemoryTreeTupleSet();
    completeDelta = (InMemoryTupleSet) context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
    currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    if (stage == 0) return;
    tuple.copyTo(currentTuple);
    if (!completeDelta.contains(currentTuple)) {
      currentDelta.add(currentTuple);
      currentTuple = TupleFactory.newTuple(new TLong(), new TLong(), new TLong());
    }
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    switch (stage) {
    case 0:
      stop0(context, actionOutput);
      break;
    case 1:
      stop1(context, actionOutput);
      break;
    }
  }

  private void stop0(ActionContext context, ActionOutput actionOutput) throws Exception {
    executeOneForwardChainIterationAndRestartFromStage(context, actionOutput);
  }

  private void stop1(ActionContext context, ActionOutput actionOutput) throws Exception {
    saveCurrentDelta(context);
    if (!currentDelta.isEmpty()) {
      // Repeat the process (execute a new iteration) considering the current delta
      executeOneForwardChainIterationAndRestartFromStage(context, actionOutput);
    } else {
      // Move to the second stage of the algorithm.
      List<ActionConf> actions = new ArrayList<ActionConf>();
      // FIXME: currently we re-derive everything.
      // Implement the incremental algorithm as in the paper! 
      removeAllInMemoryTuplesFromBTree(context);
      cleanInMemoryTuples(context);
      ActionsHelper.readEverythingFromBTree(actions);
      ActionsHelper.runIncrAddController(1, actions);
      actionOutput.branch(actions);
    }
  }

  private void executeOneForwardChainIterationAndRestartFromStage(ActionContext context, ActionOutput actionOutput) throws Exception {
    updateAndSaveCompleteDelta(context);
    List<ActionConf> actions = new ArrayList<ActionConf>();
    ActionsHelper.runIncrRulesParallelExecution(actions);
    ActionsHelper.runCollectToNode(actions);
    ActionsHelper.runRemoveDuplicates(actions);
    ActionsHelper.runIncrRemoveControllerInStage(1, actions);
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

  private void cleanInMemoryTuples(ActionContext context) {
    currentDelta = new InMemoryTreeTupleSet();
    completeDelta = new InMemoryTreeTupleSet();
    context.putObjectInCache(Consts.CURRENT_DELTA_KEY, currentDelta);
    context.putObjectInCache(Consts.COMPLETE_DELTA_KEY, completeDelta);
  }

}
