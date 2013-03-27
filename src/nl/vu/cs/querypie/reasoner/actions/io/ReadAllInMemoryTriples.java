package nl.vu.cs.querypie.reasoner.actions.io;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;

public class ReadAllInMemoryTriples extends Action {
	public static void addToChain(List<ActionConf> actions, String inMemoryTriplesKey) {
		ActionConf a = ActionFactory.getActionConf(ReadAllInMemoryTriples.class);
		a.setParamString(ReadAllInMemoryTriples.IN_MEMORY_KEY, inMemoryTriplesKey);
		actions.add(a);
	}

	public static final int IN_MEMORY_KEY = 0;
	private TupleSet inMemorySet;

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		for (Tuple t : inMemorySet) {
			actionOutput.output(t);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(IN_MEMORY_KEY, "in memory key", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		String inMemoryKey = getParamString(IN_MEMORY_KEY);
		inMemorySet = (TupleSet) context.getObjectFromCache(inMemoryKey);
		assert (inMemorySet != null);
	}

}
