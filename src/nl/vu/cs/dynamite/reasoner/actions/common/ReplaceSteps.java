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

	private final SimpleData[] supportKey = new SimpleData[3];
	private final Tuple tSupportKey = TupleFactory.newTuple(supportKey);
	private final SimpleData[] supportTuple = new SimpleData[4];

	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		tmp = (Map<Tuple, Integer>) context
				.getObjectFromCache(Consts.TMP_REMOVALS);
		supportTuple[3] = newStep;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		supportKey[0] = tuple.get(0);
		supportKey[1] = tuple.get(1);
		supportKey[2] = tuple.get(2);
		Integer s = tmp.get(tSupportKey);
		if (s != null) {
			newStep.setValue(s);
			supportTuple[0] = supportKey[0];
			supportTuple[1] = supportKey[1];
			supportTuple[2] = supportKey[2];
			actionOutput.output(supportTuple);
		} else {
			actionOutput.output(tuple);
		}
	}
}
