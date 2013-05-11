package nl.vu.cs.dynamite.compression.smart;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;

public class ExtractContext extends Action {

	SContext c;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		c = new SContext();
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		// TODO: This action extracts all sort of information that could be
		// useful to drive a semantic compression
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// TODO Auto-generated method stub
		super.stopProcess(context, actionOutput);
	}
}
