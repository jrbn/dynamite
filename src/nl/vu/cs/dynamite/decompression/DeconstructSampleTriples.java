package nl.vu.cs.dynamite.decompression;

import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.dynamite.storage.StandardTerms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeconstructSampleTriples extends Action {

	static Logger log = LoggerFactory.getLogger(DeconstructSampleTriples.class);

	TLong[] triple = new TLong[3];
	Tuple tuple = TupleFactory.newTuple();
	Map<Long, String> predefinedValues = StandardTerms.getNumberToText();

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		// Read the triple
		triple[0] = (TLong) inputTuple.get(0);
		triple[1] = (TLong) inputTuple.get(1);
		triple[2] = (TLong) inputTuple.get(2);

		if (!predefinedValues.containsKey(triple[0].getValue())) {
			inputTuple.set(triple[0]);
			output.output(inputTuple);
		}

		if (!predefinedValues.containsKey(triple[1].getValue())) {
			inputTuple.set(triple[1]);
			output.output(inputTuple);
		}

		if (!predefinedValues.containsKey(triple[2].getValue())) {
			inputTuple.set(triple[2]);
			output.output(inputTuple);
		}
	}
}
