package nl.vu.cs.querypie.reasoner.rules;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Rule {

	static final Logger log = LoggerFactory.getLogger(Rule.class);

	public int id;
	public int type;
	public Pattern HEAD;

	public Rule(int id, String head) {
		HEAD = Utils.parsePattern(head);
		this.id = id;
	}
}