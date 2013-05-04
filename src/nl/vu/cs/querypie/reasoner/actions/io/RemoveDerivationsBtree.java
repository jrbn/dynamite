package nl.vu.cs.querypie.reasoner.actions.io;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.support.Consts;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveDerivationsBtree extends Action {

	protected static final Logger log = LoggerFactory
			.getLogger(RemoveDerivationsBtree.class);

	public static void addToChain(ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(RemoveDerivationsBtree.class);
		actions.add(c);
	}

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private long removedTriples;
	private long notRemovedTriples;
	private final byte[] key = new byte[24];

	private int encode(long l1, long l2, long l3) {
		return in.encode(key, l1, l2, l3);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		in = (BTreeInterface) ReasoningContext.getInstance().getKB();
		spo = in.openWritingSession(context, DBType.SPO);
		sop = in.openWritingSession(context, DBType.SOP);
		pso = in.openWritingSession(context, DBType.PSO);
		pos = in.openWritingSession(context, DBType.POS);
		osp = in.openWritingSession(context, DBType.OSP);
		ops = in.openWritingSession(context, DBType.OPS);
		removedTriples = notRemovedTriples = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		Object obj = context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		removeAllTuplesInSet(context, obj);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
		context.incrCounter("Removed triples", removedTriples);
		context.incrCounter("Triples that could still be derived",
				notRemovedTriples);
	}

	private void removeAllTuplesInSet(ActionContext context, Object set) {

		if (set instanceof TupleSet) {

			Map<Tuple, Integer> tmpSteps = new HashMap<Tuple, Integer>();

			for (Tuple tuple : (TupleSet) set) {
				long s = ((TLong) tuple.get(0)).getValue();
				long p = ((TLong) tuple.get(1)).getValue();
				long o = ((TLong) tuple.get(2)).getValue();

				if (log.isDebugEnabled()) {
					log.debug("Possibly removing " + s + " " + p + " " + o);
				}

				int len = encode(s, p, o);
				int originalStep = 0;
				if ((originalStep = spo.removeIfStepNonZero(key, len)) == 0) {
					removedTriples++;
				} else {
					tmpSteps.put(tuple, originalStep);
				}

				len = encode(s, o, p);
				sop.removeIfStepNonZero(key, len);

				len = encode(p, o, s);
				pos.removeIfStepNonZero(key, len);

				len = encode(p, s, o);
				pso.removeIfStepNonZero(key, len);

				len = encode(o, s, p);
				osp.removeIfStepNonZero(key, len);

				len = encode(o, p, s);
				ops.removeIfStepNonZero(key, len);
			}

			context.putObjectInCache(Consts.TMP_REMOVALS, tmpSteps);
		} else {
			for (Map.Entry<Tuple, Integer> entry : ((TupleStepMap) set)
					.entrySet()) {
				Tuple tuple = entry.getKey();
				int value = entry.getValue();
				long s = ((TLong) tuple.get(0)).getValue();
				long p = ((TLong) tuple.get(1)).getValue();
				long o = ((TLong) tuple.get(2)).getValue();

				int len = encode(s, p, o);
				boolean removed = spo.decreaseOrRemove(key, len, value);

				len = encode(s, o, p);
				sop.decreaseOrRemove(key, len, value);

				len = encode(p, o, s);
				pos.decreaseOrRemove(key, len, value);

				len = encode(p, s, o);
				pso.decreaseOrRemove(key, len, value);

				len = encode(o, s, p);
				osp.decreaseOrRemove(key, len, value);

				len = encode(o, p, s);
				ops.decreaseOrRemove(key, len, value);

				if (removed) {
					removedTriples++;
				} else {
					notRemovedTriples++;
				}
			}
		}
	}
}
