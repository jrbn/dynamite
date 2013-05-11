package nl.vu.cs.dynamite.index;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SumCounts extends Action {

	protected static final Logger log = LoggerFactory
			.getLogger(SumCounts.class);

	private final TInt tStep = new TInt();
	private final TInt tCount = new TInt();

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// First three terms are the parts of the tuple. The Bag contains the
		// step and count.
		TBag bag = (TBag) tuple.get(3);

		int minStep = Integer.MAX_VALUE;
		int count = 0;
		for (Tuple t : bag) {
			int step = ((TInt) t.get(0)).getValue();
			if (step < minStep) {
				minStep = step;
			}
			count += ((TInt) t.get(1)).getValue();
		}

		tStep.setValue(minStep);
		tCount.setValue(count);
		actionOutput.output(tuple.get(0), tuple.get(1), tuple.get(2), tStep,
				tCount);
	}
}
