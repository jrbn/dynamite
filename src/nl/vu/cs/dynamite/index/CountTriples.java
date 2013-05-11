package nl.vu.cs.dynamite.index;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;

public class CountTriples extends Action {

	private long count = 0;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		count = 0;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		count++;
		output.output(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		context.putObjectInCache("countTriples", new Long(count));
	}

}
