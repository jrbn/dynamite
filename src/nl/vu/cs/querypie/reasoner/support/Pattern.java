package nl.vu.cs.querypie.reasoner.support;

public class Pattern {

	private final Term[] terms = new Term[3];

	public void setTerm(int pos, Term term) {
		term.copyTo(terms[pos]);
	}
}
