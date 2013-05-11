package nl.vu.cs.dynamite.compression;

import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;

public class TriplePartitioner extends Partitioner {

	@Override
	public int partition(Tuple tuple) {
		TLong tripleId = (TLong) tuple.get(0);
		return (tripleId.hashCode() & Integer.MAX_VALUE) % npartitions;
	}
}
