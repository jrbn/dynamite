package nl.vu.cs.querypie.reasoner.rules;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleParser {

	static final Logger log = LoggerFactory.getLogger(RuleParser.class);

	public Rule[] parseRules(String file) throws Exception {
		log.info("Start parsing the ruleset file");

		List<Rule> output = new ArrayList<>();

		BufferedReader f = new BufferedReader(new FileReader(new File(file)));
		String line = f.readLine();
		int i = 0;

		while (line != null) {
			String[] type = line.split("\t");
			// Get the type
			int typeRule = Integer.valueOf(type[0]);

			// Parse the signature
			String signature = type[1];
			String[] split = signature.split(" :- ");
			String head = split[0].substring(1, split[0].length() - 1);

			if (typeRule == 1) {
				// There is only one pattern to change
				String generic_pattern = split[1];
				output.add(new Rule1(i, head, generic_pattern));
			} else if (typeRule == 2) {
				// There are two patterns to join
				String[] patterns = split[1].split(",");
				output.add(new Rule2(i, head, patterns[0], patterns[1]));
			}

			line = f.readLine();
			i++;
		}
		f.close();

		return output.toArray(new Rule[output.size()]);
	}
}
