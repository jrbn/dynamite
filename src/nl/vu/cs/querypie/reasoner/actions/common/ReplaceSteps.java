package nl.vu.cs.querypie.reasoner.actions.common;

import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.support.Consts;

public class ReplaceSteps extends Action {

	private Map<Tuple, Integer> tmp;
	private final TInt newStep = new TInt();
	private final Tuple supportTuple = TupleFactory.newTuple(new SimpleData[4]);

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		tmp = (Map<Tuple, Integer>) context
				.getObjectFromCache(Consts.TMP_REMOVALS);
		supportTuple.set(newStep, 3);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		supportTuple.set(tuple.get(0), 0);
		supportTuple.set(tuple.get(1), 1);
		supportTuple.set(tuple.get(2), 2);
		if (tmp.containsKey(supportTuple)) {
			Integer s = tmp.get(supportTuple);
			newStep.setValue(s);
			actionOutput.output(supportTuple);
		} else {
			actionOutput.output(tuple);
		}
	}

}
