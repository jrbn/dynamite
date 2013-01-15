package nl.vu.cs.querypie.reasoner.utils;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.reasoner.SpecialTerms;
import nl.vu.cs.querypie.storage.RDFTerm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils extends arch.utils.Utils {

	static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static Pattern parsePattern(String sp) {
		Pattern p = new Pattern();
		String[] sterms = sp.split(" ");
		for (int i = 0; i < 3; ++i) {
			RDFTerm t = new RDFTerm();
			String st = sterms[i];
			if (st.charAt(0) == '?') {
				t.setName(st.substring(1));
				t.setValue(SpecialTerms.ALL_RESOURCES);
			} else {
				t.setName(null);
				t.setValue(Long.valueOf(st));
			}
			p.p[i] = t;
		}
		return p;

	}
}