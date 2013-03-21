package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

public class IncrRulesParallelExecution extends Action {

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    List<Rule> rulesOnlySchema = new ArrayList<Rule>();
    List<Rule> rulesSchemaGenerics = new ArrayList<Rule>();

    // Determine the rules that have information in delta and organize them according to their type
    extractSchemaRulesWithInformationInDelta(context, rulesOnlySchema, rulesSchemaGenerics);

    // FIXME Currently always execute the first schema only rules, which is wrong.. Must execute the first "flagged" rules
    // Execute all schema rules in parallel (on different branches)
    executeSchemaOnlyRulesInParallel(rulesOnlySchema.size(), context, actionOutput);

    // FIXME This operation is necessary, but is this the right place to perform it?
    reloadPrecomputationOnRules(rulesSchemaGenerics, context);

    // Read all the delta triples and apply all the rules with a single antecedent
    executeGenericRules(context, actionOutput);

    // Execute rules that require a map and a reduce
    executePrecomGenericRules(context, actionOutput);

    // If some schema is changed, re-apply the rules over the entire input which is affected
    for (Rule r : rulesSchemaGenerics) {
      // Get all the possible "join" values that match the schema
      Pattern pattern = new Pattern();
      r.getGenericBodyPatterns()[0].copyTo(pattern);
      int[][] shared_pos = r.getSharedVariablesGen_Precomp();
      Tuples tuples = r.getFlaggedPrecomputedTuples();
      Collection<Long> possibleValues = tuples.getSortedSet(shared_pos[0][1]);
      for (long v : possibleValues) {
        pattern.setTerm(shared_pos[0][0], new Term(v));
        executePrecomGenericRulesForPattern(pattern, context, actionOutput);
      }
    }

  }

  private void extractSchemaRulesWithInformationInDelta(ActionContext context, List<Rule> rulesOnlySchema, List<Rule> rulesSchemaGenerics) throws Exception {
    InMemoryTupleSet set = (InMemoryTupleSet) context.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
    Map<Pattern, Collection<Rule>> patterns = ReasoningContext.getInstance().getRuleset().getPrecomputedPatternSet();
    for (Pattern p : patterns.keySet()) {
      // Skip if it does not include schema information
      if (set.getSubset(p).isEmpty()) {
        continue;
      }
      for (Rule rule : patterns.get(p)) {
        if (rule.getGenericBodyPatterns().length == 0) {
          rulesOnlySchema.add(rule);
        } else {
          rulesSchemaGenerics.add(rule);
        }
      }
    }
  }

  private void reloadPrecomputationOnRules(Collection<Rule> rules, ActionContext context) {
    for (Rule r : rules) {
      r.reloadPrecomputation(ReasoningContext.getInstance(), context, true);
    }
  }

  private void executeGenericRules(ActionContext context, ActionOutput actionOutput) throws Exception {
    List<ActionConf> actions = new ArrayList<ActionConf>();
    runQueryInputLayer(actions);
    runReadAllTuplesFromDelta(actions);
    runGenericRuleExecutor(actions);
    actionOutput.branch(actions);
  }

  private void executeSchemaOnlyRulesInParallel(int numRules, ActionContext context, ActionOutput actionOutput) throws Exception {
    for (int i = 0; i < numRules; ++i) {
      List<ActionConf> actions = new ArrayList<ActionConf>();
      runQueryInputLayer(actions);
      runPrecomputeRuleExectorForRule(i, actions);
      actionOutput.branch(actions);
    }
  }

  private void executePrecomGenericRules(ActionContext context, ActionOutput actionOutput) throws Exception {
    List<ActionConf> actions = new ArrayList<ActionConf>();
    runQueryInputLayer(actions);
    runReadAllTuplesFromDelta(actions);
    runMap(actions);
    runGroupBy(actions);
    runReduce(actions);
    actionOutput.branch(actions);
  }

  private void executePrecomGenericRulesForPattern(Pattern pattern, ActionContext context, ActionOutput actionOutput) throws Exception {
    List<ActionConf> actions = new ArrayList<ActionConf>();
    runReadFromBTree(pattern, actions);
    runMap(actions);
    runGroupBy(actions);
    runReduce(actions);
    actionOutput.branch(actions);
  }

  private void runQueryInputLayer(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
    a.setParamInt(QueryInputLayer.I_INPUTLAYER, nl.vu.cs.ajira.utils.Consts.DUMMY_INPUT_LAYER_ID);
    a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
    actions.add(a);
  }

  private void runPrecomputeRuleExectorForRule(int ruleId, List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
    a.setParamInt(PrecomputedRuleExecutor.RULE_ID, ruleId);
    a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG, true);
    actions.add(a);
  }

  private void runReadAllTuplesFromDelta(List<ActionConf> actions) {
    actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));
  }

  private void runGenericRuleExecutor(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(GenericRuleExecutor.class);
    actions.add(a);
  }

  private void runMap(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(PrecompGenericMap.class);
    a.setParamBoolean(PrecompGenericMap.INCREMENTAL_FLAG, true);
    actions.add(a);
  }

  private void runGroupBy(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(GroupBy.class);
    a.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
    a.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(), TByte.class.getName(), TLong.class.getName());
    actions.add(a);
  }

  private void runReduce(List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(PrecompGenericReduce.class);
    a.setParamBoolean(PrecompGenericMap.INCREMENTAL_FLAG, true);
    actions.add(a);
  }

  private void runReadFromBTree(Pattern pattern, List<ActionConf> actions) {
    ActionConf a = ActionFactory.getActionConf(ReadFromBtree.class);
    Query query = new Query(new TLong(pattern.getTerm(0).getValue()), new TLong(pattern.getTerm(1).getValue()), new TLong(pattern.getTerm(2).getValue()));
    a.setParamWritable(ReadFromBtree.TUPLE, query);
    actions.add(a);
  }
}