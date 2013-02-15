package nl.vu.cs.querypie.reasoner.rules;

import nl.vu.cs.querypie.reasoner.support.Pattern;

public class Rule {

	private int id;
	private Pattern head;
	private Pattern[] body;

	public Rule(int id, Pattern head, Pattern[] body) {
		this.id = id;
		this.head = head;
		this.body = body;
	}

	public int getId() {
		return id;
	}

	public Pattern getHead() {
		return head;
	}

	public Pattern[] getBodyPatterns() {
		return body;
	}
}
