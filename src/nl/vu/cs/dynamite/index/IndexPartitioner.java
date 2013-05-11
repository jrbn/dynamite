package nl.vu.cs.dynamite.index;

import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.Tuple;

public class IndexPartitioner extends Partitioner {

	@Override
	public int partition(Tuple tuple) {
		return ((TByte) tuple.get(0)).getValue();
	}
}
