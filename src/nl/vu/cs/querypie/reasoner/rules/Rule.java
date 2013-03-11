package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.Utils;
import nl.vu.cs.querypie.reasoner.support.sets.Tuples;

public class Rule {

  private final int id;
  private final Pattern head;
  private final Pattern[] precomputedPatterns;
  private final Pattern[] genericPatterns;

  // Contains the set of precomputed triples
  private Tuples precomputedTuples = null;
  private Collection<String> precomputedSignatures = null;

  // Positions shared variables in the first generic pattern that are used in
  // the head
  private int[][] pos_gen_head = null;

  // Positions shared variables of the precomputed patters that appear in the
  // head of the rule
  private int[][] pos_head_precomp = null;

  // Positions of the shared variables between the first generic pattern and
  // the precomputed triples. This is used to filter generic triples that will
  // not produce any derivation
  private int[][] pos_gen_precomp = null;

  public Rule(int id, Pattern head, Pattern[] body) {
    this.id = id;
    this.head = head;

    List<Pattern> precomp = new ArrayList<Pattern>();
    List<Pattern> gen = new ArrayList<Pattern>();
    for (Pattern p : body) {
      if (p.isPrecomputed()) {
        precomp.add(p);
      } else {
        gen.add(p);
      }
    }

    precomputedPatterns = precomp.toArray(new Pattern[precomp.size()]);
    genericPatterns = gen.toArray(new Pattern[gen.size()]);
  }

  public int getId() {
    return id;
  }

  public Pattern getHead() {
    return head;
  }

  public Pattern[] getPrecomputedBodyPatterns() {
    return precomputedPatterns;
  }

  public Pattern[] getGenericBodyPatterns() {
    return genericPatterns;
  }

  public int[][] getSharedVariablesGen_Precomp() {
    return pos_gen_precomp;
  }

  public int[][] getSharedVariablesGen_Head() {
    return pos_gen_head;
  }

  public int[][] getSharedVariablesHead_Precomp() {
    return pos_head_precomp;
  }

  public Tuples getPrecomputedTuples() {
    return precomputedTuples;
  }

  public void reloadPrecomputation(ReasoningContext c, ActionContext context) {
    if (precomputedPatterns != null) try {
      precomputedTuples = c.getSchemaManager().getTuples(precomputedPatterns, context);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void init(ReasoningContext c) {

    // If there are more precomputed patterns, precompute the join in
    // memory.
    if (precomputedPatterns != null && precomputedPatterns.length > 0) {
      precomputedSignatures = Utils.concatenateVariables(precomputedPatterns);

      // Calculate the positions of the precomputed patterns that appear
      // in the head
      pos_head_precomp = Utils.getPositionSharedVariables(head, precomputedSignatures);
    }

    if (genericPatterns != null && genericPatterns.length > 0) {
      // Calculate the positions of the shared variables between the head
      // and the first generic pattern (it will be the key of the "map"
      // phase)
      pos_gen_head = Utils.getPositionSharedVariables(genericPatterns[0], head);

      // Calculate the positions of the shared variables between the first
      // generic pattern and the precomputed triples
      if (precomputedPatterns != null && precomputedPatterns.length > 0) {
        pos_gen_precomp = Utils.getPositionSharedVariables(genericPatterns[0], precomputedSignatures);
      }
    }

  }
}
