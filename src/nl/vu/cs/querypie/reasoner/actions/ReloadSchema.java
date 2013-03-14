package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class ReloadSchema extends Action {

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// Reload all the schema
		if (context.isPrincipalBranch()) {
			ReasoningContext rc = ReasoningContext.getInstance();
			for (Rule rule : rc.getRuleset().getAllRulesWithSchemaAndGeneric()) {
				rule.reloadPrecomputation(rc, context);
			}
		}
	}
}
