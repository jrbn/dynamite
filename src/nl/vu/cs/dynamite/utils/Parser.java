package nl.vu.cs.dynamite.utils;

public class Parser {

	public static String[] parseTriple(String triple, String fileId,
			boolean rewriteBlankNodes) throws Exception {
		String[] values = new String[3];

		// Parse subject
		if (triple.startsWith("<")) {
			values[0] = triple.substring(0, triple.indexOf('>') + 1);
		} else { // Is a bnode
			if (rewriteBlankNodes) {
				values[0] = "_:" + sanitizeBlankNodeName(fileId)
						+ triple.substring(2, triple.indexOf(' '));
			} else {
				values[0] = triple.substring(0, triple.indexOf(' '));
			}
		}

		triple = triple.substring(triple.indexOf(' ') + 1);
		// Parse predicate. It can be only a URI
		values[1] = triple.substring(0, triple.indexOf('>') + 1);

		// Parse object
		triple = triple.substring(values[1].length() + 1);
		if (triple.startsWith("<")) { // URI
			values[2] = triple.substring(0, triple.indexOf('>') + 1);
		} else if (triple.charAt(0) == '"') { // Literal
			values[2] = triple.substring(0,
					triple.substring(1).indexOf('"') + 2);
			triple = triple.substring(values[2].length(), triple.length());
			values[2] += triple.substring(0, triple.indexOf(' '));
		} else { // Bnode
			if (rewriteBlankNodes) {
				values[2] = "_:" + sanitizeBlankNodeName(fileId)
						+ triple.substring(2, triple.indexOf(' '));
			} else {
				values[2] = triple.substring(0, triple.indexOf(' '));
			}
		}
		return values;
	}

	private static String sanitizeBlankNodeName(String filename) {
		StringBuffer ret = new StringBuffer(filename.length());
		if (!filename.isEmpty()) {
			char charAt0 = filename.charAt(0);
			if (Character.isLetter(charAt0))
				ret.append(charAt0);
		}
		for (int i = 1; i < filename.length(); i++) {
			char ch = filename.charAt(i);
			if (Character.isLetterOrDigit(ch)) {
				ret.append(ch);
			}
		}
		return ret.toString();
	}
}
