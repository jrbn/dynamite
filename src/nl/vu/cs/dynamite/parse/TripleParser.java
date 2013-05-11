package nl.vu.cs.dynamite.parse;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.dynamite.utils.Parser;

public class TripleParser extends Action {

	TString[] triple = { new TString(), new TString(), new TString() };
	long malformedTriples, parsedTriples;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		malformedTriples = parsedTriples = 0;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		TString input = (TString) inputTuple.get(0);
		String[] urls = Parser.parseTriple(input.getValue(), "", false);
		if (urls != null && urls.length == 3) {
			triple[0].setValue(urls[0]);
			triple[1].setValue(urls[1]);
			triple[2].setValue(urls[2]);
			output.output(triple);
			parsedTriples++;
		} else {
			malformedTriples++;
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		context.incrCounter("malformed triples", malformedTriples);
		context.incrCounter("parsed triples", parsedTriples);
	}
}
