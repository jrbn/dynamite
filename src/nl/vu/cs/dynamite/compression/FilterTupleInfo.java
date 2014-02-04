package nl.vu.cs.dynamite.compression;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class FilterTupleInfo extends Action {
	
	TString uri = new TString();
	TLong tripleId = new TLong();
	TByte pos = new TByte();
	Tuple outputTuple = TupleFactory.newTuple(uri, tripleId, pos);

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		if (tuple.getNElements() == 2) {
			uri.setValue(((TString)tuple.get(0)).getValue());
			// this is in fact the compressed ID
			tripleId.setValue(((TLong)tuple.get(1)).getValue());
			pos.setValue(0);
		} else {
			uri.setValue(((TString)tuple.get(1)).getValue());
			tripleId.setValue(((TLong)tuple.get(2)).getValue());
			pos.setValue(((TByte)tuple.get(3)).getValue());
		}
		actionOutput.output(outputTuple);
	}

}
