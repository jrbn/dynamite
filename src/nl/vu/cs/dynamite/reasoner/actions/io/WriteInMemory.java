package nl.vu.cs.dynamite.reasoner.actions.io;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.dynamite.storage.inmemory.TupleSet;
import nl.vu.cs.dynamite.storage.inmemory.TupleSetImpl;

public class WriteInMemory extends Action {
	public static void addToChain(String inMemoryTriplesKey, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(WriteInMemory.class);
		a.setParamString(WriteInMemory.S_IN_MEMORY_KEY, inMemoryTriplesKey);
		actions.add(a);
	}

	public static final int S_IN_MEMORY_KEY = 0;
	private TupleSet inMemorySet;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_IN_MEMORY_KEY, "S_IN_MEMORY_KEY", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		String inMemoryKey = getParamString(S_IN_MEMORY_KEY);
		inMemorySet = (TupleSet) context.getObjectFromCache(inMemoryKey);
		if (inMemorySet == null) {
			inMemorySet = new TupleSetImpl();
			context.putObjectInCache(inMemoryKey, inMemorySet);
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		Tuple tupleCopy = TupleFactory.newTuple();
		tuple.copyTo(tupleCopy);
		inMemorySet.add(tupleCopy);
		actionOutput.output(tuple);
	}
}
