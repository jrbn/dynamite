package nl.vu.cs.querypie.reasoner.actions;

import java.util.ArrayList;
import java.util.List;

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
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

public class IncrRulesParallelExecution extends Action {

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {

  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    // Create n branches, one per every schema rule
    Rule[] rules = ReasoningContext.getInstance().getRuleset().getAllSchemaOnlyRules();

    // FIXME: this operation is necessary, but I'm not sure if this is the right place to execute it
    // Please check!
    Rule[] allRules = ReasoningContext.getInstance().getRuleset().getAllRulesWithSchemaAndGeneric();
    for (int i = 0; i < allRules.length; ++i) {
      allRules[i].reloadPrecomputation(ReasoningContext.getInstance(), context, true);
    }

    for (int i = 0; i < rules.length; ++i) {
      List<ActionConf> actions = new ArrayList<ActionConf>();

      ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
      a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
      a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
      actions.add(a);

      a = ActionFactory.getActionConf(PrecomputedRuleExecutor.class);
      a.setParamInt(PrecomputedRuleExecutor.RULE_ID, i);
      a.setParamBoolean(PrecomputedRuleExecutor.INCREMENTAL_FLAG, true);
      actions.add(a);

      actionOutput.branch(actions);
    }

    /******
     * Read all the delta triples and apply on them all the rules with a single antecedent.
     ******/
    List<ActionConf> actions = new ArrayList<ActionConf>();
    ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
    a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
    a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
    actions.add(a);

    // Read all the triples from the delta and stream them to the rest of
    // the chain
    actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));

    // Apply all the rules on them
    a = ActionFactory.getActionConf(GenericRuleExecutor.class);
    actions.add(a);

    actionOutput.branch(actions);

    /*****
     * Apply the rules that require a map and reduce.
     *****/
    actions = new ArrayList<ActionConf>();
    a = ActionFactory.getActionConf(QueryInputLayer.class);
    a.setParamInt(QueryInputLayer.I_INPUTLAYER, Consts.DUMMY_INPUT_LAYER_ID);
    a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
    actions.add(a);

    // Read all the triples from the delta and stream them to the rest of the chain
    actions.add(ActionFactory.getActionConf(ReadAllInmemoryTriples.class));

    // Map
    a = ActionFactory.getActionConf(PrecompGenericMap.class);
    a.setParamBoolean(PrecompGenericMap.INCREMENTAL_FLAG, true);
    actions.add(a);

    // Group by
    a = ActionFactory.getActionConf(GroupBy.class);
    a.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
    a.setParamStringArray(GroupBy.TUPLE_FIELDS, TByteArray.class.getName(), TByte.class.getName(), TLong.class.getName());
    actions.add(a);

    // Reduce
    a = ActionFactory.getActionConf(PrecompGenericReduce.class);
    a.setParamBoolean(PrecompGenericMap.INCREMENTAL_FLAG, true);
    actions.add(a);

    actionOutput.branch(actions);

    /****
     * TODO: If some schema is changed, reapply the rules over the entire input which is affected
     */

  }
}
