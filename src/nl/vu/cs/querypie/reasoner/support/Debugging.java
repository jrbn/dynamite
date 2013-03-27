package nl.vu.cs.querypie.reasoner.support;

import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;

public class Debugging extends Action {

	public static void addToChain(List<ActionConf> actions) {
		ActionConf c = ActionFactory.getActionConf(Debugging.class);
		actions.add(c);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		// TODO define parameters for writing to file
		DebuggingUtils.printDerivations(ReasoningContext.getInstance().getKB(), context);
	}
}
