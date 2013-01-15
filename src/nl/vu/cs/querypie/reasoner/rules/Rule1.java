package nl.vu.cs.querypie.reasoner.rules;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule1 extends Rule {

	static final Logger log = LoggerFactory.getLogger(Rule1.class);

	public Pattern[] PRECOMPS;

	public Rule1(int id, String head, String[] precomps) throws Exception {
		super(id, head);
		type = 1;

		PRECOMPS = new Pattern[precomps.length];
		for (int i = 0; i < precomps.length; ++i) {
			PRECOMPS[i] = Utils.parsePattern(precomps[i]);
		}
	}
}