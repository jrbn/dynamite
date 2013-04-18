package nl.vu.cs.querypie.reasoner.actions.io;

import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class ReadAllInMemoryTriples extends Action {
	public static void addToChain(String inMemoryTriplesKey, ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(ReadAllInMemoryTriples.class);
		a.setParamString(ReadAllInMemoryTriples.IN_MEMORY_KEY, inMemoryTriplesKey);
		actions.add(a);
	}

	public static final int IN_MEMORY_KEY = 0;

	private Set<Tuple> inMemorySet = null;
	private Map<Tuple, Integer> inMemorySetWithCounter = null;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(IN_MEMORY_KEY, "in memory key", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		String inMemoryKey = getParamString(IN_MEMORY_KEY);
		Object obj = context.getObjectFromCache(inMemoryKey);
		if (obj instanceof TupleSet) {
			inMemorySet = (TupleSet) obj;
		} else {
			inMemorySetWithCounter = (TupleStepMap) obj;
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		if (inMemorySet != null) {
			readFromSet(actionOutput);
		} else {
			readFromSetWithCounter(actionOutput);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		inMemorySet = null;
		inMemorySetWithCounter = null;
	}

	private void readFromSet(ActionOutput actionOutput) throws Exception {
		for (Tuple t : inMemorySet) {
			actionOutput.output(t);
		}
	}

	private void readFromSetWithCounter(ActionOutput actionOutput) throws Exception {
		SimpleData[] supportTuple = new SimpleData[5];
		TInt count = new TInt();
		supportTuple[4] = count;
		for (Map.Entry<Tuple, Integer> entry : inMemorySetWithCounter.entrySet()) {
			Tuple key = entry.getKey();
			supportTuple[0] = key.get(0);
			supportTuple[1] = key.get(1);
			supportTuple[2] = key.get(2);
			supportTuple[3] = key.get(3);
			count.setValue(entry.getValue());
			actionOutput.output(supportTuple);
		}
	}

}
