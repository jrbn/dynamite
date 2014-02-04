package nl.vu.cs.dynamite.compression;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;


public class RemoveURI extends Action {

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		List<Tuple> tupleList = new ArrayList<Tuple>();
		TLong compressedId = new TLong();
		TBag values = (TBag) tuple.get(1);

		for (Tuple value : values) {
			if (((TByte) value.get(1)).getValue() == 0) {
				compressedId.setValue(((TLong) value.get(0)).getValue());
			} else {
				Tuple t = TupleFactory.newTuple(
						new TLong(((TLong)value.get(0)).getValue()),
						compressedId,
						new TByte(((TByte)value.get(1)).getValue()));	
				tupleList.add(t);
			}
		}
		
		for (Tuple t: tupleList) {
			actionOutput.output(t);
		}
		
	}

}
