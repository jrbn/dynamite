package nl.vu.cs.querypie.reasoner.rules;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule2 extends Rule1 {

	static final Logger log = LoggerFactory.getLogger(Rule2.class);

	public Pattern PRECOMPUTED_PATTERN;

	public Rule2(int id, String head, String precomputed_pattern,
			String generic_pattern) throws Exception {
		super(id, head, generic_pattern);
		type = 2;
		PRECOMPUTED_PATTERN = Utils.parsePattern(precomputed_pattern);
	}
}