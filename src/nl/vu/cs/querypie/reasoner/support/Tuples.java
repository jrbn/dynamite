package nl.vu.cs.querypie.reasoner.support;

import java.util.Collection;

public class Tuples {

	private Collection<String> signature;

	public Tuples(Collection<String> signature) {
		this.signature = signature;
	}

	public Collection<String> getSignature() {
		return signature;
	}
}
