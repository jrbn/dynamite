package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;

public class ReadAllInmemoryTriples extends Action {
  static final int IN_MEMORY_KEY = 0;
  private InMemoryTupleSet inMemorySet;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(IN_MEMORY_KEY, "in memory key", null, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    String inMemoryKey = getParamString(IN_MEMORY_KEY);
    inMemorySet = (InMemoryTupleSet) context.getObjectFromCache(inMemoryKey);
    assert (inMemorySet != null);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    for (Tuple t : inMemorySet) {
      actionOutput.output(t);
    }
  }
}
