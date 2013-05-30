package nl.vu.cs.dynamite.compression;

import java.util.Random;

import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class URLsPartitioner extends Partitioner {

	Random r = new Random();

	@Override
	public int partition(Tuple tuple) {
		TString url = (TString) tuple.get(0);
		if (url.getValue().charAt(0) != '#') {
			int hash = url.hashCode();
			return (hash ^ (hash >> 5)) % npartitions;
		}
		return r.nextInt(npartitions);
	}
}
