package nl.vu.cs.dynamite.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class TriplesRemoveDuplicates extends Action {

	private boolean first;
	private final Tuple tuple = TupleFactory.newTuple();

	@Override
	public void startProcess(ActionContext context) throws Exception {
		first = true;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {

		if (first) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
			first = false;
		} else if (!inputTuple.get(0).equals(tuple.get(0))
				|| !inputTuple.get(1).equals(tuple.get(1))
				|| !inputTuple.get(2).equals(tuple.get(2))) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
		}
	}

}
