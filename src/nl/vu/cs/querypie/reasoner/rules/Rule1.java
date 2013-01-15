package nl.vu.cs.querypie.reasoner.rules;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule1 extends Rule {

	static final Logger log = LoggerFactory.getLogger(Rule1.class);

	public Pattern GENERIC_PATTERN;

	public Rule1(int id, String head, String generic) throws Exception {
		super(id, head);
		type = 1;
		GENERIC_PATTERN = Utils.parsePattern(generic);
	}
}