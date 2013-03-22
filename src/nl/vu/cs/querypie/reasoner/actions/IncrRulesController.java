package nl.vu.cs.querypie.reasoner.actions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.support.FilterHiddenFiles;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.querypie.reasoner.common.Consts;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTreeTupleSet;
import nl.vu.cs.querypie.storage.inmemory.InMemoryTupleSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrRulesController extends Action {

	static final Logger log = LoggerFactory
			.getLogger(IncrRulesController.class);

	public static final int S_DELTA_DIR = 0;
	public static final int I_STAGE = 1;

	private String deltaDir = null;
	private int stage = 0;
	private InMemoryTupleSet currentDelta = null;
	private InMemoryTupleSet oldDelta = null;
	private Tuple currentTuple = null;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(S_DELTA_DIR, "dir of the update", null, true);
		conf.registerParameter(I_STAGE, "stage computation", 0, false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		deltaDir = getParamString(S_DELTA_DIR);
		stage = getParamInt(I_STAGE);
		switch (stage) {
		case 1:
			oldDelta = (InMemoryTupleSet) context
					.getObjectFromCache(Consts.IN_MEMORY_TUPLE_SET_KEY_OLD);
			if (oldDelta == null) {
				oldDelta = (InMemoryTupleSet) context
						.getObjectFromCache(Consts.IN_MEMORY_TUPLE_SET_KEY);
			}
			currentDelta = new InMemoryTreeTupleSet();
			currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
					new TLong());
			break;
		}
	}

	private void process1(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) {
		tuple.copyTo(currentTuple);
		if (!oldDelta.contains(currentTuple)) {
			currentDelta.add(currentTuple);
			currentTuple = TupleFactory.newTuple(new TLong(), new TLong(),
					new TLong());
		}

	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		switch (stage) {
		case 1:
			process1(tuple, context, actionOutput);
			break;
		}
	}

	private void stop0(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// Check if the content of the delta is already in the cache. If it is
		// not, then parse it from the file.
		InMemoryTupleSet delta = (InMemoryTupleSet) context
				.getObjectFromCache(Consts.IN_MEMORY_TUPLE_SET_KEY);
		if (delta == null) {
			delta = parseTriplesFromFile(deltaDir);
			context.putObjectInCache(Consts.IN_MEMORY_TUPLE_SET_KEY, delta);
		}

		// Apply all the rules in parallel just once
		List<ActionConf> actions = new ArrayList<ActionConf>();
		actions.add(ActionFactory
				.getActionConf(IncrRulesParallelExecution.class));

		// Collect all the derivations on one node
		ActionConf c = ActionFactory.getActionConf(CollectToNode.class);
		c.setParamStringArray(CollectToNode.TUPLE_FIELDS,
				TLong.class.getName(), TLong.class.getName(),
				TLong.class.getName());
		c.setParamBoolean(CollectToNode.SORT, true);
		actions.add(c);

		// Remove the duplicates
		actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));

		// Update the delta and go to the next stage
		c = ActionFactory.getActionConf(IncrRulesController.class);
		c.setParamInt(I_STAGE, 1);
		c.setParamString(IncrRulesController.S_DELTA_DIR, deltaDir);
		actions.add(c);

		// Branch
		actionOutput.branch(actions);
	}

	private void stop1(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (currentDelta.size() > 0) {

			// Copy the new triples in the total container
			oldDelta.addAll(currentDelta);
			// Replace the delta with the new triples
			context.putObjectInCache(Consts.IN_MEMORY_TUPLE_SET_KEY,
					currentDelta);
			// Store the old delta in another data structure
			context.putObjectInCache(Consts.IN_MEMORY_TUPLE_SET_KEY_OLD,
					oldDelta);

			// Repeat the process
			stop0(context, actionOutput);
		} else {
			// TODO: Move to the second stage of the algorithm.
			// 1) Remove everything in Delta
			// 2) Recompute the remaining derivation
		}

		currentDelta = null;
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		switch (stage) {
		case 0:
			stop0(context, actionOutput);
			break;
		case 1:
			stop1(context, actionOutput);
			break;
		}
	}

	private static InMemoryTupleSet parseTriplesFromFile(String input) {
		InMemoryTupleSet set = new InMemoryTreeTupleSet();
		try {
			List<File> files = new ArrayList<File>();

			File fInput = new File(input);
			if (fInput.isDirectory()) {
				for (File child : fInput.listFiles(new FilterHiddenFiles()))
					files.add(child);
			} else {
				files.add(fInput);
			}

			for (File file : files) {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line = null;
				while ((line = reader.readLine()) != null) {
					// Parse the line
					String[] sTriple = line.split(" ");
					TLong[] triple = { new TLong(), new TLong(), new TLong() };
					triple[0].setValue(Long.valueOf(sTriple[0]));
					triple[1].setValue(Long.valueOf(sTriple[1]));
					triple[2].setValue(Long.valueOf(sTriple[2]));
					set.add(TupleFactory.newTuple(triple));
				}
				reader.close();
			}
		} catch (Exception e) {
			log.error("Error in reading the update", e);
		}
		return set;
	}
}