package nl.vu.cs.querypie.reasoner.actions;

import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;

class PrecompGenericMap extends Action {

  static final int INCREMENTAL_FLAG = 0;

  private int[][] key_positions;
  private int[][] positions_to_check;
  private int[][] pos_constants_to_check;
  private long[][] value_constants_to_check;
  private Collection<Long>[] acceptableValues;
  private List<Rule> rules;

  private final TByteArray oneKey = new TByteArray(new byte[8]);
  private final TByteArray twoKeys = new TByteArray(new byte[16]);
  private final TByte ruleID = new TByte();
  private final Tuple outputTuple = TupleFactory.newTuple();

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(INCREMENTAL_FLAG, "incremental_flag", false, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void startProcess(ActionContext context) throws Exception {
    boolean incrementalFlag = getParamBoolean(INCREMENTAL_FLAG);
    rules = ReasoningContext.getInstance().getRuleset().getAllRulesWithSchemaAndGeneric();
    key_positions = new int[rules.size()][];
    positions_to_check = new int[rules.size()][];
    acceptableValues = new Collection[rules.size()];
    pos_constants_to_check = new int[rules.size()][];
    value_constants_to_check = new long[rules.size()][];

    for (int r = 0; r < rules.size(); ++r) {
      Rule rule = rules.get(r);

      // Get the positions of the generic patterns that are used in the
      // head
      int[][] shared_vars = rule.getSharedVariablesGen_Head();
      key_positions[r] = new int[shared_vars.length];
      for (int i = 0; i < key_positions[r].length; ++i) {
        key_positions[r][i] = shared_vars[i][0];
      }

      // Get the positions in the generic variables that should be checked
      // against the schema
      shared_vars = rule.getSharedVariablesGen_Precomp();
      positions_to_check[r] = new int[shared_vars.length];
      for (int i = 0; i < positions_to_check[r].length; ++i) {
        positions_to_check[r][i] = shared_vars[i][0];
      }

      // Get the elements from the precomputed tuples that should be checked
      if (shared_vars.length > 1) {
        throw new Exception("Not implemented yet");
      }

      acceptableValues[r] = incrementalFlag ? rule.getFlaggedPrecomputedTuples().getSortedSet(shared_vars[0][1]) : rule.getAllPrecomputedTuples().getSortedSet(shared_vars[0][1]);
      pos_constants_to_check[r] = rule.getPositionsConstantGenericPattern();
      value_constants_to_check[r] = rule.getValueConstantGenericPattern();
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    for (int r = 0; r < rules.size(); r++) {
      // Does the input match with the generic pattern?
      if (!nl.vu.cs.querypie.reasoner.support.Utils.tupleMatchConstants(tuple, pos_constants_to_check[r], value_constants_to_check[r])) {
        continue;
      }
      TLong t = (TLong) tuple.get(positions_to_check[r][0]);
      if (acceptableValues[r].contains(t.getValue())) {
        ruleID.setValue(r);
        if (key_positions[r].length == 1) {
          Utils.encodeLong(oneKey.getArray(), 0, ((TLong) tuple.get(key_positions[r][0])).getValue());
          outputTuple.set(oneKey, ruleID, tuple.get(positions_to_check[r][0]));
        } else { // Two keys
          Utils.encodeLong(twoKeys.getArray(), 0, ((TLong) tuple.get(key_positions[r][0])).getValue());
          Utils.encodeLong(twoKeys.getArray(), 8, ((TLong) tuple.get(key_positions[r][1])).getValue());
          outputTuple.set(twoKeys, ruleID, tuple.get(positions_to_check[r][0]));
        }
        actionOutput.output(outputTuple);
      }
    }
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    rules = null;
    acceptableValues = null;
    key_positions = null;
    positions_to_check = null;
  }
}
