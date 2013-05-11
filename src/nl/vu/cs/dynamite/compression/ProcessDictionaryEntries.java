package nl.vu.cs.dynamite.compression;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.Tuple;

public class ProcessDictionaryEntries extends Action {

	private final static TByte POS = new TByte(0);

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple.get(1), tuple.get(0), POS);
	}

}
