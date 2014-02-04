package nl.vu.cs.dynamite.compression;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.dynamite.NewCompress;
import nl.vu.cs.ajira.data.types.TBitSet;

public class CreateBitVectors extends Action {
	Map<Integer,Integer> clusterIndexes = null;
	TString uri = new TString();
	TBitSet bitSet = new TBitSet();
	Tuple currentTuple = TupleFactory.newTuple(bitSet, uri);
	int bitSetSize = 0;
	
	@SuppressWarnings("unchecked")
	@Override
	public void startProcess(ActionContext context) throws Exception {
		clusterIndexes = (Map<Integer, Integer>) context
				.getObjectFromCache(NewCompress.CLUSTER_INDEXES);
		bitSetSize = clusterIndexes.size();
	}
	
	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput output)
			throws Exception {	
		uri.setValue(((TString) tuple.get(0)).getValue());
		TBag values = (TBag) tuple.get(1);
		BitSet bs = new BitSet(clusterIndexes.size());
		
		// set all the bits to 0
		bs.clear();
		
		// set bits corresponding to cluster indexes to one
		for (Tuple value : values) {
			int cluster = ((TInt) value.get(0)).getValue();
			bs.set(clusterIndexes.get(cluster));
		}
		bitSet.setBitSet(bs);
		output.output(currentTuple);
	}
	
}
