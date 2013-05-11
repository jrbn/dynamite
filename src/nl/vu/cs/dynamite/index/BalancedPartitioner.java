package nl.vu.cs.dynamite.index;

import java.util.Arrays;
import java.util.Comparator;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;

public class BalancedPartitioner extends Partitioner {

	private static Comparator<TLong[]> c = new java.util.Comparator<TLong[]>() {
		@Override
		public int compare(TLong[] o1, TLong[] o2) {
			int c = 0;
			for (int i = 0; i < o1.length; ++i) {
				c = o1[i].compareTo(o2[i]);
				if (c != 0)
					return c;
			}
			return c;
		}
	};

	Partitions partitions;

	@Override
	public void init(ActionContext context, int npartitions, byte[] partition_fields) {
		super.init(context,  npartitions, partition_fields);
		partitions = (Partitions) context.getObjectFromCache("partitions");
	}

	TByte index;
	TLong[] triple = new TLong[3];

	private int determinePartition(TLong[][] partitions) {
		return Arrays.binarySearch(partitions, triple, c);
	}

	@Override
	public int partition(Tuple tuple) {
		index = (TByte) tuple.get(0);
		triple[0] = (TLong) tuple.get(1);
		triple[1] = (TLong) tuple.get(2);
		triple[2] = (TLong) tuple.get(3);

		TLong[][] partitionLimits = partitions.get(index.getValue());
		int pos = determinePartition(partitionLimits);
		if (pos < 0) {
			pos = pos * -1 - 1;
		}
		return pos;
	}

}
