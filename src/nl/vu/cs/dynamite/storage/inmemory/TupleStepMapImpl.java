package nl.vu.cs.dynamite.storage.inmemory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.dynamite.storage.Pattern;

public class TupleStepMapImpl extends TreeMap<Tuple, Integer> implements
		TupleStepMap {

	private static final long serialVersionUID = 8680718692273575697L;

	private final TreeSet<Tuple> additionalIndex;

	public TupleStepMapImpl() {
		super(new PSOTupleComparator());
		additionalIndex = new TreeSet<Tuple>(new POSTupleComparator());
	}

	@Override
	public void clear() {
		super.clear();
		additionalIndex.clear();
	}

	@Override
	public synchronized Set<Tuple> getSubset(Pattern p) throws Exception {
		boolean sIsVariable = (p.getTerm(0).getName() != null);
		boolean pIsVariable = (p.getTerm(1).getName() != null);
		boolean oIsVariable = (p.getTerm(2).getName() != null);
		if (pIsVariable) {
			throw new Exception("Not implemented");
		}
		long pValue = p.getTerm(1).getValue();
		if (oIsVariable && !sIsVariable) {
			long sValue = p.getTerm(0).getValue();
			Tuple fromTuple = TupleFactory.newTuple(new TLong(sValue),
					new TLong(pValue), new TLong(0));
			Tuple toTuple = TupleFactory.newTuple(new TLong(sValue), new TLong(
					pValue), new TLong(Long.MAX_VALUE));
			return new HashSet<Tuple>(subMap(fromTuple, toTuple).keySet());

		} else if (sIsVariable && !oIsVariable) {
			long oValue = p.getTerm(2).getValue();
			Tuple fromTuple = TupleFactory.newTuple(new TLong(0), new TLong(
					pValue), new TLong(oValue));
			Tuple toTuple = TupleFactory.newTuple(new TLong(Long.MAX_VALUE),
					new TLong(pValue), new TLong(oValue));
			return new HashSet<Tuple>(
					additionalIndex.subSet(fromTuple, toTuple));
		} else if (sIsVariable && oIsVariable) {
			Tuple fromTuple = TupleFactory.newTuple(new TLong(0), new TLong(
					pValue), new TLong(0));
			Tuple toTuple = TupleFactory.newTuple(new TLong(Long.MAX_VALUE),
					new TLong(pValue), new TLong(Long.MAX_VALUE));
			return new HashSet<Tuple>(subMap(fromTuple, toTuple).keySet());
		} else {
			long sValue = p.getTerm(0).getValue();
			long oValue = p.getTerm(2).getValue();
			Tuple fromTuple = TupleFactory.newTuple(new TLong(sValue),
					new TLong(pValue), new TLong(oValue));
			Tuple toTuple = TupleFactory.newTuple(new TLong(sValue), new TLong(
					pValue), new TLong(oValue + 1));
			return new HashSet<Tuple>(subMap(fromTuple, toTuple).keySet());
		}
	}

	@Override
	public synchronized Integer put(Tuple key, Integer value) {
		int c = value;
		Integer v = get(key);
		if (v != null) {
			c += v;
		}
		additionalIndex.add(key);
		return super.put(key, c);
	}

	@Override
	public synchronized void putAll(Map<? extends Tuple, ? extends Integer> m) {
		for (Map.Entry<? extends Tuple, ? extends Integer> entry : m.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public synchronized Integer remove(Object key) {
		additionalIndex.remove(key);
		return super.remove(key);
	}
}
