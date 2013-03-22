package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;

class ReadAllInmemoryTriples extends Action {

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    InMemoryTupleSet delta = (InMemoryTupleSet) context.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
    for (Tuple t : delta) {
      actionOutput.output(t);
    }
  }
}
