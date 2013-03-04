package nl.vu.cs.querypie.reasoner.support;

public class Pattern {

	private final Term[] terms = new Term[3];
	private boolean isPrecomputed = false;

	public void setTerm(int pos, Term term) {
		term.copyTo(terms[pos]);
	}

	public Term getTerm(int pos) {
		return terms[pos];
	}

	public void setPrecomputed(boolean value) {
		isPrecomputed = value;
	}

	public boolean isPrecomputed() {
		return isPrecomputed;
	}
}
