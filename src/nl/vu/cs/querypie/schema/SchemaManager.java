package nl.vu.cs.querypie.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

public class SchemaManager {

  static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

  private final BerkeleydbLayer kb;

  public SchemaManager(BerkeleydbLayer kb) {
    this.kb = kb;
  }

  public Tuples getTuples(Pattern[] patterns, ActionContext context, boolean flaggedOnly) throws Exception {
    List<Map<String, Integer>> variablesPositions = retrieveVariablesFromPatterns(patterns);
    if (!isCurrentlySupported(variablesPositions)) {
      throw new Exception("Currently not implemented");
    }
    if (patterns.length == 1) {
      Pattern p = patterns[0];
      Set<Tuple> tuples = flaggedOnly ? retrieveAllFlaggedTuplesForPattern(p, context) : retrieveAllTuplesForPattern(p, context);
      SortedSet<Integer> variablesPositionsSet = new TreeSet<Integer>(variablesPositions.get(0).values());
      return generateTuples(tuples, variablesPositionsSet);
    } else {
      Pattern p1 = patterns[0];
      Pattern p2 = patterns[1];
      Set<Tuple> allTuples1 = retrieveAllTuplesForPattern(p1, context);
      Set<Tuple> allTuples2 = retrieveAllTuplesForPattern(p2, context);
      SortedSet<Integer> resultVariablesPositions = new TreeSet<Integer>();
      resultVariablesPositions.add(0);
      resultVariablesPositions.add(1);
      if (flaggedOnly) {
        Set<Tuple> flaggedTuples1 = retrieveAllFlaggedTuplesForPattern(p1, context);
        Set<Tuple> flaggedTuples2 = retrieveAllFlaggedTuplesForPattern(p2, context);
        Set<Tuple> result1 = joinSets(variablesPositions.get(0), flaggedTuples1, variablesPositions.get(1), allTuples2);
        Set<Tuple> result2 = joinSets(variablesPositions.get(0), allTuples1, variablesPositions.get(1), flaggedTuples2);
        result1.addAll(result2);
        return generateTuples(result1, resultVariablesPositions);
      } else {
        allTuples1.addAll(allTuples2);
        return generateTuples(allTuples1, resultVariablesPositions);
      }
    }
  }

  private List<Map<String, Integer>> retrieveVariablesFromPatterns(Pattern patterns[]) {
    List<Map<String, Integer>> variablesPositions = new ArrayList<Map<String, Integer>>();
    for (int i = 0; i < patterns.length; ++i) {
      Pattern p = patterns[i];
      Map<String, Integer> map = retrieveVariablesFromPatter(p);
      variablesPositions.add(map);
    }
    return variablesPositions;
  }

  private Map<String, Integer> retrieveVariablesFromPatter(Pattern p) {
    Map<String, Integer> variables = new HashMap<String, Integer>();
    for (int i = 0; i < 3; i++) {
      Term term = p.getTerm(i);
      String name = term.getName();
      if (name != null) {
        variables.put(name, i);
      }
    }
    return variables;
  }

  private Set<Tuple> retrieveAllTuplesForPattern(Pattern p, ActionContext context) {
    Set<Tuple> tuples = new HashSet<Tuple>();
    TLong[] query = new TLong[3];
    for (int j = 0; j < 3; ++j) {
      if (p.getTerm(j).getName() != null) {
        query[j] = new TLong(-1);
      } else {
        query[j] = new TLong(p.getTerm(j).getValue());
      }
    }
    Tuple t = TupleFactory.newTuple(query);
    TupleIterator itr = kb.getIterator(t, context);
    try {
      while (itr != null && itr.isReady() && itr.nextTuple()) {
        itr.getTuple(t);
        tuples.add(t);
      }
    } catch (Exception e) {
      log.error("Error", e);
    }
    return tuples;
  }

  private Set<Tuple> retrieveAllFlaggedTuplesForPattern(Pattern p, ActionContext context) {
    InMemoryTupleSet inMemorySet = (InMemoryTupleSet) context.getObjectFromCache(Consts.IN_MEMORY_TUPLE_SET_KEY);
    if (inMemorySet == null) {
      log.error("Unable to retrieve in-memory tuple set from cache");
    }
    Set<Tuple> result = null;
    try {
      result = inMemorySet.getSubset(p);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  private Set<Tuple> joinSets(Map<String, Integer> var1, Set<Tuple> t1, Map<String, Integer> var2, Set<Tuple> t2) {
    // Determine position of variables
    int joinVar1, joinVar2, freeVar1, freeVar2;
    joinVar1 = joinVar2 = freeVar1 = freeVar2 = 0;
    for (String key1 : var1.keySet()) {
      for (String key2 : var2.keySet()) {
        if (key1.equals(key2)) {
          joinVar1 = var1.get(key1);
          joinVar2 = var2.get(key2);
        } else {
          freeVar1 = var1.get(key1);
          freeVar2 = var2.get(key2);
        }
      }
    }
    // Order tuples according to the joining variable
    Map<SimpleData, Set<SimpleData>> firstMap = getJoinMap(t1, joinVar1, freeVar1);
    Map<SimpleData, Set<SimpleData>> secondMap = getJoinMap(t1, joinVar2, freeVar2);
    // Execute the join
    Set<Tuple> result = executeJoin(firstMap, secondMap);
    return result;
  }

  private Map<SimpleData, Set<SimpleData>> getJoinMap(Set<Tuple> t, int keyPos, int freePos) {
    Map<SimpleData, Set<SimpleData>> map = new HashMap<SimpleData, Set<SimpleData>>();
    for (Tuple tuple : t) {
      SimpleData key = tuple.get(keyPos);
      SimpleData value = tuple.get(freePos);
      if (map.containsKey(key)) {
        map.get(key).add(value);
      } else {
        Set<SimpleData> newSet = new HashSet<SimpleData>();
        newSet.add(value);
        map.put(key, newSet);
      }
    }
    return map;
  }

  // TODO: define the order of free variables in output
  private Set<Tuple> executeJoin(Map<SimpleData, Set<SimpleData>> first, Map<SimpleData, Set<SimpleData>> second) {
    Set<Tuple> result = new HashSet<Tuple>();
    for (SimpleData key : first.keySet()) {
      if (!second.containsKey(key)) continue;
      for (SimpleData freeVal1 : first.get(key)) {
        for (SimpleData freeVal2 : second.get(key)) {
          Tuple t = TupleFactory.newTuple(freeVal1, freeVal2);
          result.add(t);
        }
      }
    }
    return result;
  }

  private Tuples generateTuples(Set<Tuple> inputTuples, SortedSet<Integer> variablesPositions) {
    Collection<Long> resultList = new ArrayList<Long>();
    for (Tuple t : inputTuples) {
      for (Integer pos : variablesPositions) {
        TLong val = (TLong) t.get(pos);
        Long longVal = val.getValue();
        resultList.add(longVal);
      }
    }
    return new Tuples(variablesPositions.size(), Longs.toArray(resultList));
  }

  private boolean isCurrentlySupported(List<Map<String, Integer>> variablesPositions) {
    if (variablesPositions.size() < 1 || variablesPositions.size() > 2) return false;
    if (variablesPositions.size() == 1) {
      return true;
    } else if (variablesPositions.size() == 2) {
      Map<String, Integer> pattern1 = variablesPositions.get(0);
      Map<String, Integer> pattern2 = variablesPositions.get(1);
      if (pattern1.size() != 2 || pattern2.size() != 2) return false;
      int numConstraints = 0;
      for (String key1 : pattern1.keySet()) {
        for (String key2 : pattern2.keySet()) {
          if (key1.equals(key2)) {
            numConstraints++;
          }
        }
      }
      return (numConstraints == 1);
    } else {
      return false;
    }
  }
}
