package nl.vu.cs.querypie.reasoner.support.sets;

public class RowSet {

	long[] values;
	int start, end, length, current;

	RowSet(long[] values, int start, int end, int lenght) {
		this.values = values;
		this.start = start;
		this.end = end;
		current = start - length;
	}

	public boolean hasNext() {
		return current < end;
	}

	public void next() {
		current += length;
	}

	public long getCurrent(int pos) {
		return values[current + pos];
	}

}
