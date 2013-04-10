package nl.vu.cs.querypie.reasoner.actions.io;

import java.util.Arrays;
import java.util.Set;

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
import nl.vu.cs.querypie.reasoner.support.ParamHandler;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.TupleStepMap;

public class RemoveDerivationsBtree extends Action {
	public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(RemoveDerivationsBtree.class);
		actions.add(c);
	}

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private long removedTriples;
	private long notRemovedTriples;
	private final byte[] key = new byte[24];
	
	private byte[] encode(long l1, long l2, long l3) {
		int sz = in.encode(key, l1, l2, l3);
		if (sz < key.length) {
			return Arrays.copyOf(key, sz);
		}
		return key;
	}
	
	@Override
	public void startProcess(ActionContext context) throws Exception {
		in = (BTreeInterface) ReasoningContext.getInstance().getKB();
		spo = in.openWritingSession(DBType.SPO);
		sop = in.openWritingSession(DBType.SOP);
		pso = in.openWritingSession(DBType.PSO);
		pos = in.openWritingSession(DBType.POS);
		osp = in.openWritingSession(DBType.OSP);
		ops = in.openWritingSession(DBType.OPS);
		removedTriples = notRemovedTriples = 0;
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
		// Remove the content of the derivation from the BTrees
		Object obj = context.getObjectFromCache(Consts.COMPLETE_DELTA_KEY);
		Set<Tuple> tuplesToRemove = null;
		if (obj instanceof TupleSet) {
			tuplesToRemove = ((TupleSet) obj);
		} else if (obj instanceof TupleStepMap) {
			tuplesToRemove = ((TupleStepMap) obj).keySet();
		} else {
			throw new Exception("Unknown in memory implementation");
		}
		removeAllTuplesInSet(tuplesToRemove);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
		context.incrCounter("Removed triples", removedTriples);
		context.incrCounter("Triples that could still be derived", notRemovedTriples);
	}

	private void removeAllTuplesInSet(Set<Tuple> set) {
		for (Tuple tuple : set) {
			TLong s = (TLong) tuple.get(0);
			TLong p = (TLong) tuple.get(1);
			TLong o = (TLong) tuple.get(2);

			boolean removed = false;
			if (ParamHandler.get().isUsingCount()) {
				byte[] toRemove = encode(s.getValue(), p.getValue(), o.getValue());
				removed = spo.decreaseOrRemove(toRemove);

				toRemove = encode(s.getValue(), o.getValue(), p.getValue());
				sop.decreaseOrRemove(toRemove);

				toRemove = encode(p.getValue(), o.getValue(), s.getValue());
				pos.decreaseOrRemove(toRemove);

				toRemove = encode(p.getValue(), s.getValue(), o.getValue());
				pso.decreaseOrRemove(toRemove);

				toRemove = encode(o.getValue(), s.getValue(), p.getValue());
				osp.decreaseOrRemove(toRemove);

				toRemove = encode(o.getValue(), p.getValue(), s.getValue());
				ops.decreaseOrRemove(toRemove);
			} else {
				removed = true;
				byte[] toRemove = encode(s.getValue(), p.getValue(), o.getValue());
				spo.remove(toRemove);

				toRemove = encode(s.getValue(), o.getValue(), p.getValue());
				sop.remove(toRemove);

				toRemove = encode(p.getValue(), o.getValue(), s.getValue());
				pos.remove(toRemove);

				toRemove = encode(p.getValue(), s.getValue(), o.getValue());
				pso.remove(toRemove);

				toRemove = encode(o.getValue(), s.getValue(), p.getValue());
				osp.remove(toRemove);

				toRemove = encode(o.getValue(), p.getValue(), s.getValue());
				ops.remove(toRemove);
			}

			if (removed) {
				removedTriples++;
			} else {
				notRemovedTriples++;
			}
		}
	}
}
