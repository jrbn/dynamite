package nl.vu.cs.querypie.reasoner;

import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import arch.Arch;
import arch.submissions.Job;

public class RDFSRulesExecutor {

	private Job executeRule2(Rule2 rule) {
		return null;
	}

	public void execute(Arch arch, Rule[] rdfs_rules) throws Exception {

		// First execute the first rule of type 2. It is subproperty
		// transitivity.
		Job job = executeRule2((Rule2) rdfs_rules[0]);
		arch.waitForCompletion(job);

		// Now execute the second rule of type 2. It is subproperty inheritances
		job = executeRule2((Rule2) rdfs_rules[1]);
		arch.waitForCompletion(job);

		// Etc.
	}
}
