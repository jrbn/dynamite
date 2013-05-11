package nl.vu.cs.dynamite.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;

public class ForwardOnlyFirst extends Action {

	private boolean first;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		first = true;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (first) {
			actionOutput.output(tuple);
			first = false;
		}
	}

}
