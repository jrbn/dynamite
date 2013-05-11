package nl.vu.cs.dynamite.compression;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.dynamite.storage.Dictionary;
import nl.vu.cs.dynamite.storage.StandardTerms;
import nl.vu.cs.dynamite.storage.Dictionary.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeconstructSampleTriples extends Action {

	public static final int S_DICT_DIR = 0;

	static Logger log = LoggerFactory.getLogger(DeconstructSampleTriples.class);

	TString[] triple = new TString[3];
	Map<String, Long> alreadyExistingValues = null;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DICT_DIR, "S_DICT_DIR", null, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		String existingDir = getParamString(S_DICT_DIR);

		if (existingDir == null || !(new java.io.File(existingDir).exists())) {
			alreadyExistingValues = StandardTerms.getTextToNumber();
		} else {
			alreadyExistingValues = new HashMap<String, Long>();

			// Read the existing values from the dictionary and do not consider
			// them
			Iterator<Pair> pairs = Dictionary.readAllPairs(existingDir, "c-");
			while (pairs.hasNext()) {
				Pair p = pairs.next();
				alreadyExistingValues.put(p.value, p.key);
			}
			alreadyExistingValues.putAll(StandardTerms.getTextToNumber());
		}

	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		// Read the triple
		triple[0] = (TString) inputTuple.get(0);
		triple[1] = (TString) inputTuple.get(1);
		triple[2] = (TString) inputTuple.get(2);

		if (!alreadyExistingValues.containsKey(triple[0].getValue())) {
			output.output(triple[0]);
		}

		if (!alreadyExistingValues.containsKey(triple[1].getValue())) {
			output.output(triple[1]);
		}

		if (!alreadyExistingValues.containsKey(triple[2].getValue())) {
			output.output(triple[2]);
		}
	}
}
