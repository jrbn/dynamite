package nl.vu.cs.querypie.reasoner.actions;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.io.MemoryStorage;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

/**
 * A rules controller that execute a single step of materialization based on the
 * facts written on the knowledge base and on the derivation rules.
 * 
 * It writes the newly derived rules in memory (in a cached object)
 */
public class OneStepRulesControllerToMemory extends AbstractRulesController {
	public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf a = ActionFactory.getActionConf(OneStepRulesControllerToMemory.class);
		actions.add(a);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		cleanInMemoryContainer(context, Consts.COMPLETE_DELTA_KEY);
		cleanInMemoryContainer(context, Consts.CURRENT_DELTA_KEY);
		ActionSequence actions = new ActionSequence();
		if (!ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules().isEmpty()) {
			applyRulesSchemaOnly(actions, MemoryStorage.IN_MEMORY, Integer.MIN_VALUE);
			applyRulesWithGenericPatternsInABranch(actions, MemoryStorage.IN_MEMORY, Integer.MIN_VALUE);
		} else {
			applyRulesWithGenericPatterns(actions, MemoryStorage.IN_MEMORY, Integer.MIN_VALUE);
		}
		ActionsHelper.collectToNode(actions);
		actionOutput.branch(actions);
	}

	private void cleanInMemoryContainer(ActionContext context, String key) {
		Object obj = context.getObjectFromCache(key);
		if (obj == null) {
			return;
		} else if (obj instanceof TupleSet) {
			((TupleSet) obj).clear();
		} else if (obj instanceof TupleStepMap) {
			((TupleStepMap) obj).clear();
		}
	}
}