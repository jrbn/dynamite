package nl.vu.cs.dynamite.index;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TByte;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;

public class ConstructPartitionTable extends Action {

	public static final int I_NPARTITIONS = 0;
	public static final int I_SAMPLE_RATE = 1;

	private int numberPartitions = -1;
	private int sample = -1;
	private long triplesPerPartition;

	private long actualCounter;
	private int actualIndexValue;
	private List<TLong[]> actualIndexPartition;
	private Partitions partitions;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_NPARTITIONS, "I_NPARTITIONS", -1, false);
		conf.registerParameter(I_SAMPLE_RATE, "I_SAMPLE_RATE", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		numberPartitions = getParamInt(I_NPARTITIONS);
		sample = getParamInt(I_SAMPLE_RATE);

		// Retrieve the count of triples calculated before
		long c = -1;
		c = (Long) context.getObjectFromCache("countTriples");

		if (!context.isLocalMode()) {
			List<Object[]> counts = context
					.retrieveCacheObjects("countTriples");
			if (counts != null) {
				for (Object[] objs : counts) {
					c += (Long) objs[0];
				}
			}
		}

		if (c == -1) {
			throw new Exception("No counts are available");
		}

		// Calculate the number of triples per partition

		if (numberPartitions == -1) {
			numberPartitions = context.getNumberNodes();
		}
		triplesPerPartition = c * sample / 100 / numberPartitions;
		actualCounter = 0;
		actualIndexValue = Partitions.partition_ids[0].getValue();
		actualIndexPartition = new ArrayList<TLong[]>();
		partitions = new Partitions();
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		TByte index = (TByte) inputTuple.get(0);
		actualCounter++;

		if (actualIndexValue != index.getValue()) {
			partitions.add(actualIndexValue, actualIndexPartition
					.toArray(new TLong[actualIndexPartition.size()][]));
			actualCounter = 0;
			actualIndexPartition = new ArrayList<TLong[]>();
			actualIndexValue = index.getValue();
		} else {
			if (actualCounter > triplesPerPartition
					&& actualIndexPartition.size() < numberPartitions - 1) {
				TLong[] newTriple = { new TLong(), new TLong(), new TLong() };
				((TLong) inputTuple.get(1)).copyTo(newTriple[0]);
				((TLong) inputTuple.get(2)).copyTo(newTriple[1]);
				((TLong) inputTuple.get(3)).copyTo(newTriple[2]);
				actualIndexPartition.add(newTriple);
				actualCounter = 0;
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		partitions.add(actualIndexValue, actualIndexPartition
				.toArray(new TLong[actualIndexPartition.size()][]));
		context.putObjectInCache("partitions", partitions);
		context.broadcastCacheObjects("partitions");
	}
}
