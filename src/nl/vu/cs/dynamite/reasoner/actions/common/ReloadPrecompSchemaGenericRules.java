package nl.vu.cs.dynamite.reasoner.actions.common;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.dynamite.ReasoningContext;

public class ReloadPrecompSchemaGenericRules extends Action {

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		ReasoningContext.getInstance().getRuleset()
				.reloadPrecomputationSchemaGeneric(context, true, true);
	}
}
