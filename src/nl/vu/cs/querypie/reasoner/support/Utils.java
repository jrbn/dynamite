package nl.vu.cs.querypie.reasoner.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils extends nl.vu.cs.ajira.utils.Utils {

	static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static Pattern parsePattern(String sp) {
		Pattern p = new Pattern();
		String[] sterms = sp.split(" ");
		for (int i = 0; i < 3; ++i) {
			Term t = new Term();
			String st = sterms[i];
			if (st.charAt(0) == '?') {
				t.setName(st.substring(1));
			} else {
				t.setName(null);
				t.setValue(Long.valueOf(st));
			}
			p.setTerm(i, t);
		}

		if (sp.endsWith("*")) {
			p.setPrecomputed(true);
		}
		return p;

	}
}