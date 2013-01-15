package nl.vu.cs.querypie.reasoner.rules;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule2 extends Rule1 {

	static final Logger log = LoggerFactory.getLogger(Rule2.class);

	public Pattern GENERIC_PATTERN;

	public Rule2(int id, String head, String[] precomps, String generic_pattern)
			throws Exception {
		super(id, head, precomps);
		GENERIC_PATTERN = Utils.parsePattern(generic_pattern);
	}

}