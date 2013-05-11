package nl.vu.cs.dynamite.storage;

import java.util.ArrayList;
import java.util.Arrays;
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

	public int[] getPositionVariables() {
		int[] vars = new int[3];
		int nVars = 0;
		for (int i = 0; i < 3; ++i) {
			Term t = terms[i];
			if (t.getName() != null) {
				vars[nVars++] = i;
			}
		}
		return Arrays.copyOf(vars, nVars);
	}

	@Override
	public String toString() {
		return terms[0] + " " + terms[1] + " " + terms[2];
	}

	public String toStringWithVarsAsStar() {
		String t = "";
		for (int i = 0; i < terms.length; ++i) {
			if (terms[i].getName() != null) {
				t += "* ";
			} else {
				t += terms[i].getValue() + " ";
			}
		}
		return t.substring(0, t.length() - 1);
	}

	public void copyTo(Pattern query) {
		for (int i = 0; i < terms.length; ++i) {
			terms[i].copyTo(query.terms[i]);
		}
	}
}
