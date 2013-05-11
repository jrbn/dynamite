package nl.vu.cs.dynamite.decompression;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class ExtractTextFromPopularURIs extends Action {

	TString txtValue = new TString();
	Set<Long> commonValues = new HashSet<Long>();
	boolean checked = false;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		commonValues = null;
		checked = false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (!checked) {
			commonValues = (Set<Long>) context
					.getObjectFromCache("popularURIs");
			checked = true;
		}

		if (commonValues != null) {
			TLong numberValue = (TLong) inputTuple.get(0);
			if (commonValues.contains(numberValue.getValue())) {
				output.output(inputTuple);
			}
		}
	}
}
