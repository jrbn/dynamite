package nl.vu.cs.querypie.reasoner.actions.io;

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

public class RemoveDerivationsBtree extends Action {
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
		removeAllTuplesInSet(obj);
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

	private void removeAllTuplesInSet(Object set) {

		if (set instanceof TupleSet) {
			for (Tuple tuple : (TupleSet) set) {
				TLong s = (TLong) tuple.get(0);
				TLong p = (TLong) tuple.get(1);
				TLong o = (TLong) tuple.get(2);

				int len = encode(s.getValue(), p.getValue(), o.getValue());
				spo.remove(key, len);

				len = encode(s.getValue(), o.getValue(), p.getValue());
				sop.remove(key, len);

				len = encode(p.getValue(), o.getValue(), s.getValue());
				pos.remove(key, len);

				len = encode(p.getValue(), s.getValue(), o.getValue());
				pso.remove(key, len);

				len = encode(o.getValue(), s.getValue(), p.getValue());
				osp.remove(key, len);

				len = encode(o.getValue(), p.getValue(), s.getValue());
				ops.remove(key, len);

				removedTriples++;
			}
		} else {
			for (Map.Entry<Tuple, Integer> entry : ((TupleStepMap) set)
					.entrySet()) {
				Tuple tuple = entry.getKey();
				TLong s = (TLong) tuple.get(0);
				TLong p = (TLong) tuple.get(1);
				TLong o = (TLong) tuple.get(2);

				int len = encode(s.getValue(), p.getValue(), o.getValue());
				boolean removed = spo.decreaseOrRemove(key, len,
						entry.getValue());

				len = encode(s.getValue(), o.getValue(), p.getValue());
				sop.decreaseOrRemove(key, len, entry.getValue());

				len = encode(p.getValue(), o.getValue(), s.getValue());
				pos.decreaseOrRemove(key, len, entry.getValue());

				len = encode(p.getValue(), s.getValue(), o.getValue());
				pso.decreaseOrRemove(key, len, entry.getValue());

				len = encode(o.getValue(), s.getValue(), p.getValue());
				osp.decreaseOrRemove(key, len, entry.getValue());

				len = encode(o.getValue(), p.getValue(), s.getValue());
				ops.decreaseOrRemove(key, len, entry.getValue());

				if (removed) {
					removedTriples++;
				} else {
					notRemovedTriples++;
				}
			}
		}
	}
}
