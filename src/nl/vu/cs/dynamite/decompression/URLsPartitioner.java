package nl.vu.cs.dynamite.decompression;

import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;

public class URLsPartitioner extends Partitioner {

	@Override
	public int partition(Tuple tuple) {
		TLong value = (TLong) tuple.get(0);
		return (int) ((value.getValue() >> 48) % npartitions);
	}
}
