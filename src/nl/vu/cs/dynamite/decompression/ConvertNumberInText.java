package nl.vu.cs.dynamite.decompression;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class ConvertNumberInText extends Action {

	private TString textValue;

	@Override
	public void startProcess(ActionContext context) throws Exception {
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		TByte flag = (TByte) inputTuple.get(1);

		switch (flag.getValue()) {
		case 0:
			textValue = (TString) inputTuple.get(2);
			break;
		case 1:
			TByte position = (TByte) inputTuple.get(2);
			TLong tripleID = (TLong) inputTuple.get(3);
			output.output(tripleID, position, textValue);
			break;
		case 2:
			tripleID = (TLong) inputTuple.get(0);
			position = (TByte) inputTuple.get(2);
			textValue = (TString) inputTuple.get(3);
			output.output(tripleID, position, textValue);
			break;
		}
	}
}
