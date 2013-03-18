package nl.vu.cs.querypie.reasoner.actions;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.Term;
import nl.vu.cs.querypie.reasoner.support.sets.Tuples;

public class PrecompGenericReduce extends Action {

  private int[][][] pos_head_precomps;
  private int[][][] pos_gen_precomps;
  private int[][][] pos_gen_head;
  private Tuples[] precompTuples;

  private TLong[][] outputTuples;
  private int[] counters;
  private Rule[] rules;
  private final Set<Tuple> duplicates = new HashSet<Tuple>();
  private Tuple supportTuple = TupleFactory.newTuple();

  @Override
  public void startProcess(ActionContext context) throws Exception {
    rules = ReasoningContext.getInstance().getRuleset().getAllRulesWithSchemaAndGeneric();
    counters = new int[rules.length];
    pos_head_precomps = new int[rules.length][][];
    pos_gen_precomps = new int[rules.length][][];
    pos_gen_head = new int[rules.length][][];
    precompTuples = new Tuples[rules.length];
    outputTuples = new TLong[rules.length][];

    for (int m = 0; m < rules.length; ++m) {
      Rule rule = rules[m];
      pos_head_precomps[m] = rule.getSharedVariablesHead_Precomp();
      pos_gen_precomps[m] = rule.getSharedVariablesGen_Precomp();
      pos_gen_head[m] = rule.getSharedVariablesGen_Head();
      precompTuples[m] = rule.getPrecomputedTuples();
      if (pos_gen_precomps[m].length > 1) {
        throw new Exception("Not supported");
      }

      // Fill the outputTriple with the constants that come from the head
      // of the rule
      Pattern head = rule.getHead();
      outputTuples[m] = new TLong[3];
      for (int i = 0; i < 3; ++i) {
        Term t = head.getTerm(i);
        if (t.getName() == null) {
          outputTuples[m][i] = new TLong(t.getValue());
        } else {
          outputTuples[m][i] = new TLong();
        }
      }
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    duplicates.clear();
    TByteArray key = (TByteArray) tuple.get(0);

    // Perform the join between the "value" part of the triple and the
    // precomputed tuples. Notice that this works only if there is one
    // element to join.
    TBag values = (TBag) tuple.get(tuple.getNElements() - 1);

    int currentRule = -1;
    long currentJoinValue = -1;
    for (Tuple t : values) {
      TByte rule = (TByte) t.get(0);
      TLong elementToJoin = (TLong) t.get(1);
      if (rule.getValue() == currentRule && currentJoinValue == elementToJoin.getValue()) {
        continue;
      } else {
        currentRule = rule.getValue();
        currentJoinValue = elementToJoin.getValue();
        // First copy the "key" in the output triple.
        for (int i = 0; i < pos_gen_head[currentRule].length; ++i) {
          outputTuples[currentRule][pos_gen_head[currentRule][i][1]].setValue(Utils.decodeLong(key.getArray(), 8 * i));
        }
      }

      Collection<long[]> set = precompTuples[currentRule].get(pos_gen_precomps[currentRule][0][1], currentJoinValue);
      if (set != null) {
        for (long[] row : set) {
          // Get current values
          for (int i = 0; i < pos_head_precomps[currentRule].length; ++i) {
            outputTuples[currentRule][pos_head_precomps[currentRule][i][0]].setValue(row[pos_head_precomps[currentRule][i][1]]);
          }
          supportTuple.set(outputTuples[currentRule]);
          if (!duplicates.contains(supportTuple)) {
            duplicates.add(supportTuple);
            supportTuple = TupleFactory.newTuple();
            actionOutput.output(outputTuples[currentRule]);
            counters[currentRule]++;
          }
        }
      }
    }
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    pos_head_precomps = null;
    pos_gen_precomps = null;
    pos_gen_head = null;
    precompTuples = null;
    outputTuples = null;
    for (int i = 0; i < counters.length; ++i) {
      if (counters[i] > 0) {
        context.incrCounter("derivation-rule-" + rules[i].getId(), counters[i]);
      }
    }
  }
}
