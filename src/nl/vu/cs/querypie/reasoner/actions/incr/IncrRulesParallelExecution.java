package nl.vu.cs.querypie.reasoner.actions.incr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.actions.common.ActionsHelper;
import nl.vu.cs.querypie.reasoner.actions.io.ReadAllInMemoryTriples;
import nl.vu.cs.querypie.reasoner.actions.io.ReadFromBtree;
import nl.vu.cs.querypie.reasoner.actions.rules.GenericRuleExecutor;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Consts;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

public class IncrRulesParallelExecution extends Action {
	public static void addToChain(int outputStep, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(IncrRulesParallelExecution.class);
		c.setParamInt(I_OUTPUT_STEP, outputStep);
		actions.add(c);
	}

	public static final int I_OUTPUT_STEP = 0;

	private int outputStep;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_OUTPUT_STEP, "I_OUTPUT_STEP",
				Integer.MIN_VALUE, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		outputStep = getParamInt(I_OUTPUT_STEP);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		Set<Integer> rulesOnlySchema = new HashSet<Integer>();
		Set<Rule> rulesSchemaGenerics = new HashSet<Rule>();
		// Determine the rules that have information in delta and organize them
		// according to their type
		extractSchemaRulesWithInformationInDelta(context, rulesOnlySchema,
				rulesSchemaGenerics);

		if (rulesOnlySchema.size() > 0) {
			ReasoningContext.getInstance().getRuleset()
					.reloadPrecomputationSchema(context, false, true);
			ActionsHelper.executeSchemaRulesInParallel(rulesOnlySchema,
					Integer.MIN_VALUE, outputStep, true, actionOutput);
		}

		// Apply all rules with a single antecedent on delta triples
		executeGenericRulesOnDelta(context, actionOutput);

		// Apply all rules that require a map and a reduce on delta triples
		ReasoningContext.getInstance().getRuleset()
				.reloadPrecomputationSchemaGeneric(context, true, true);
		executePrecomGenericRulesOnDelta(context, actionOutput);

		if (rulesSchemaGenerics.size() > 0) {
			executePrecomGenericRulesOnDeltaAndSchema(context, actionOutput);
			// If some schema is changed, re-apply the rules over the entire
			// input which is affected
			for (Rule rule : rulesSchemaGenerics) {
				Pattern pattern = getQueryPattern(rule);
				Collection<Long> possibleValues = getValuesMatchingTheSchema(
						context, rule);
				int varPos = getVariablePosition(rule);
				for (long v : possibleValues) {
					pattern.setTerm(varPos, new Term(v));
					executePrecomGenericRulesForPattern(pattern, context,
							actionOutput);
				}
			}
		}
	}

	private void extractSchemaRulesWithInformationInDelta(
			ActionContext context, Set<Integer> rulesOnlySchema,
			Set<Rule> rulesSchemaGenerics) throws Exception {
		TupleSet set = (TupleSet) context
				.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
		Map<Pattern, Collection<Rule>> patterns = ReasoningContext
				.getInstance().getRuleset().getPrecomputedPatternSet();
		List<Rule> allSchemaOnlyRules = ReasoningContext.getInstance()
				.getRuleset().getAllSchemaOnlyRules();
		List<Rule> selectedSchemaOnlyRules = new ArrayList<Rule>();
		for (Pattern p : patterns.keySet()) {
			// Skip if it does not include schema information
			if (set.getSubset(p).isEmpty()) {
				continue;
			}
			for (Rule rule : patterns.get(p)) {
				if (rule.getGenericBodyPatterns().isEmpty()) {
					selectedSchemaOnlyRules.add(rule);
					if (log.isDebugEnabled()) {
						log.debug("Adding rule " + rule.getId()
								+ " to selectedSchemaOnlyRules");
					}
				} else {
					rulesSchemaGenerics.add(rule);
					if (log.isDebugEnabled()) {
						log.debug("Adding rule " + rule.getId()
								+ " to rulesSchemaGenerics");
					}
				}
			}
		}
		for (int i = 0; i < allSchemaOnlyRules.size(); ++i) {
			Rule r = allSchemaOnlyRules.get(i);
			if (selectedSchemaOnlyRules.contains(r)) {
				if (log.isDebugEnabled()) {
					log.debug("Adding rule " + r.getId()
							+ " to rulesOnlySchema");
				}
				rulesOnlySchema.add(i);
			}
		}
	}

	private void executeGenericRulesOnDelta(ActionContext context,
			ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(Consts.CURRENT_DELTA_KEY, actions);
		GenericRuleExecutor.addToChain(Integer.MIN_VALUE, outputStep, actions);
		actionOutput.branch(actions);
	}

	// FIXME: Merge this method with executePrecomGenericRulesOnDeltaAndSchema,
	// by refactoring the SchemaManager
	private void executePrecomGenericRulesOnDelta(ActionContext context,
			ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(Consts.CURRENT_DELTA_KEY, actions);
		ActionsHelper.mapReduce(Integer.MIN_VALUE, outputStep, false, actions);
		actionOutput.branch(actions);
	}

	// FIXME: Merge this method with executePrecomGenericRulesOnDelta, by
	// refactoring the SchemaManager
	private void executePrecomGenericRulesOnDeltaAndSchema(
			ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		ActionsHelper.readFakeTuple(actions);
		ReadAllInMemoryTriples.addToChain(Consts.CURRENT_DELTA_KEY, actions);
		ActionsHelper.mapReduce(Integer.MIN_VALUE, outputStep, true, actions);
		actionOutput.branch(actions);
	}

	private void executePrecomGenericRulesForPattern(Pattern pattern,
			ActionContext context, ActionOutput actionOutput) throws Exception {
		ActionSequence actions = new ActionSequence();
		ReadFromBtree.addToChain(pattern, actions);
		ActionsHelper.mapReduce(Integer.MIN_VALUE, outputStep, true, actions);
		actionOutput.branch(actions);
	}

	private Pattern getQueryPattern(Rule rule) {
		Pattern pattern = new Pattern();
		rule.getGenericBodyPatterns().get(0).copyTo(pattern);
		return pattern;
	}

	private Collection<Long> getValuesMatchingTheSchema(ActionContext context,
			Rule rule) {
		int[][] shared_pos = rule.getSharedVariablesGen_Precomp();
		Tuples tuples = rule.getFlaggedPrecomputedTuples(context);
		return tuples.getSortedSet(shared_pos[0][1]);
	}

	private int getVariablePosition(Rule rule) {
		int[][] shared_pos = rule.getSharedVariablesGen_Precomp();
		return shared_pos[0][0];
	}
}
