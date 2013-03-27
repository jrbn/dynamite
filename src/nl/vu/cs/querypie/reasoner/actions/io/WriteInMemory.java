package nl.vu.cs.querypie.reasoner.actions.io;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleSetImpl;

public class WriteInMemory extends Action {
	public static final int IN_MEMORY_KEY = 0;
	private TupleSet inMemorySet;

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		Tuple tupleCopy = TupleFactory.newTuple();
		tuple.copyTo(tupleCopy);
		inMemorySet.add(tupleCopy);
		actionOutput.output(tuple);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(IN_MEMORY_KEY, "in memory key", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		String inMemoryKey = getParamString(IN_MEMORY_KEY);
		inMemorySet = (TupleSet) context.getObjectFromCache(inMemoryKey);
		if (inMemorySet == null) {
			inMemorySet = new TupleSetImpl();
			context.putObjectInCache(inMemoryKey, inMemorySet);
		}
	}
}
