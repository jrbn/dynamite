package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTreeTupleSet;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;

public class ClearInMemoryTupleSet extends Action {

  static final int IN_MEMORY_KEY = 0;

  private String inMemoryKey;

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(IN_MEMORY_KEY, "in memory key", null, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    inMemoryKey = getParamString(IN_MEMORY_KEY);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    InMemoryTupleSet set = new InMemoryTreeTupleSet();
    context.putObjectInCache(inMemoryKey, set);
  }
}
