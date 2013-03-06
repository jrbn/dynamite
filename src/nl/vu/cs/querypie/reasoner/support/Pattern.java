package nl.vu.cs.querypie.reasoner.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Pattern {

	private final Term[] terms = { new Term(), new Term(), new Term() };
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

	public Collection<String> getVariables() {
		List<String> vars = new ArrayList<String>();
		for (Term t : terms) {
			if (t.getName() != null) {
				// Check for duplicates
				boolean unique = true;
				for (String s : vars) {
					if (s.equals(t.getName())) {
						unique = false;
					}
				}
				if (unique)
					vars.add(t.getName());
			}
		}
		return vars;
	}

	@Override
	public String toString() {
		return terms[0] + " " + terms[1] + " " + terms[2];
	}
}
