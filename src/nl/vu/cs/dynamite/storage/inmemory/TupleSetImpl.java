package nl.vu.cs.dynamite.storage.inmemory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.dynamite.storage.Pattern;

/**
 * Implementation of InMemoryTupleSet based on multiple TreeSets.
 */
public class TupleSetImpl implements TupleSet {

	private final TreeSet<Tuple> psoSet;
	private final TreeSet<Tuple> posSet;

	public TupleSetImpl() {
		psoSet = new TreeSet<Tuple>(new PSOTupleComparator());
		posSet = new TreeSet<Tuple>(new POSTupleComparator());
	}

	@Override
	public int size() {
		return psoSet.size();
	}

	@Override
	public boolean isEmpty() {
		return psoSet.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return psoSet.contains(o);
	}

	@Override
	public Iterator<Tuple> iterator() {
		return psoSet.iterator();
	}

	@Override
	public Object[] toArray() {
		return psoSet.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return psoSet.toArray(a);
	}

	@Override
	public synchronized boolean add(Tuple e) {
		psoSet.add(e);
		return posSet.add(e);
	}

	@Override
	public synchronized boolean remove(Object o) {
		psoSet.remove(o);
		return posSet.remove(o);
	}

	@Override
	public synchronized boolean containsAll(Collection<?> c) {
		return psoSet.containsAll(c);
	}

	@Override
	public synchronized boolean addAll(Collection<? extends Tuple> c) {
		psoSet.addAll(c);
		return posSet.addAll(c);
	}

	@Override
	public synchronized boolean retainAll(Collection<?> c) {
		psoSet.retainAll(c);
		return posSet.retainAll(c);
	}

	@Override
	public synchronized boolean removeAll(Collection<?> c) {
		psoSet.removeAll(c);
		return posSet.retainAll(c);
	}

	@Override
	public void clear() {
		posSet.clear();
		psoSet.clear();
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
			return new HashSet<Tuple>(psoSet.subSet(fromTuple, toTuple));
		} else if (sIsVariable && !oIsVariable) {
			long oValue = p.getTerm(2).getValue();
			Tuple fromTuple = TupleFactory.newTuple(new TLong(0), new TLong(
					pValue), new TLong(oValue));
			Tuple toTuple = TupleFactory.newTuple(new TLong(Long.MAX_VALUE),
					new TLong(pValue), new TLong(oValue));
			return new HashSet<Tuple>(posSet.subSet(fromTuple, toTuple));
		} else if (sIsVariable && oIsVariable) {
			Tuple fromTuple = TupleFactory.newTuple(new TLong(0), new TLong(
					pValue), new TLong(0));
			Tuple toTuple = TupleFactory.newTuple(new TLong(Long.MAX_VALUE),
					new TLong(pValue), new TLong(Long.MAX_VALUE));
			return new HashSet<Tuple>(psoSet.subSet(fromTuple, toTuple));
		} else {
			long sValue = p.getTerm(0).getValue();
			long oValue = p.getTerm(2).getValue();
			Tuple fromTuple = TupleFactory.newTuple(new TLong(sValue),
					new TLong(pValue), new TLong(oValue));
			Tuple toTuple = TupleFactory.newTuple(new TLong(sValue), new TLong(
					pValue), new TLong(oValue + 1));
			return new HashSet<Tuple>(psoSet.subSet(fromTuple, toTuple));
		}
	}
}

class PSOTupleComparator implements Comparator<Tuple> {
	@Override
	public int compare(Tuple t1, Tuple t2) {
		// TODO Currently I'm considering only triples (plus step). Define a new
		// (less
		// expensive) kind of objects?
		assert (t1.getNElements() == 4 && t2.getNElements() == 4);
		// Order by p-s-o
		long p1 = ((TLong) t1.get(1)).getValue();
		long p2 = ((TLong) t2.get(1)).getValue();
		if (p1 < p2) {
			return -1;
		} else if (p1 > p2) {
			return 1;
		} else {
			long s1 = ((TLong) t1.get(0)).getValue();
			long s2 = ((TLong) t2.get(0)).getValue();
			if (s1 < s2) {
				return -1;
			} else if (s1 > s2) {
				return 1;
			} else {
				long o1 = ((TLong) t1.get(2)).getValue();
				long o2 = ((TLong) t2.get(2)).getValue();
				if (o1 < o2) {
					return -1;
				} else if (o1 > o2) {
					return 1;
				} else {
					return 0;
				}
			}
		}
	}
}

class POSTupleComparator implements Comparator<Tuple> {
	@Override
	public int compare(Tuple t1, Tuple t2) {
		// TODO Currently I'm considering only triples (plus step). Define a new
		// (less
		// expensive) kind of objects?
		assert (t1.getNElements() == 4 && t2.getNElements() == 4);
		// Order by p-o-s
		long p1 = ((TLong) t1.get(1)).getValue();
		long p2 = ((TLong) t2.get(1)).getValue();
		if (p1 < p2) {
			return -1;
		} else if (p1 > p2) {
			return 1;
		} else {
			long o1 = ((TLong) t1.get(2)).getValue();
			long o2 = ((TLong) t2.get(2)).getValue();
			if (o1 < o2) {
				return -1;
			} else if (o1 > o2) {
				return 1;
			} else {
				long s1 = ((TLong) t1.get(0)).getValue();
				long s2 = ((TLong) t2.get(0)).getValue();
				if (s1 < s2) {
					return -1;
				} else if (s1 > s2) {
					return 1;
				} else {
					return 0;
				}
			}
		}
	}
}
