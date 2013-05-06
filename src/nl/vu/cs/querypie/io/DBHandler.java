package nl.vu.cs.querypie.io;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.storage.BTreeInterface;
import nl.vu.cs.querypie.storage.DBType;
import nl.vu.cs.querypie.storage.WritingSession;

public class DBHandler {
	public String DBHandlerKey = "__DB_HANLDER_KEY";

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private final byte[] key = new byte[24];
	private boolean open;

	public DBHandler() {
		open = false;
	}

	public void open(ActionContext context) {
		if (open) {
			return;
		}
		in = (BTreeInterface) ReasoningContext.getInstance().getKB();
		spo = in.openWritingSession(context, DBType.SPO);
		sop = in.openWritingSession(context, DBType.SOP);
		pso = in.openWritingSession(context, DBType.PSO);
		pos = in.openWritingSession(context, DBType.POS);
		osp = in.openWritingSession(context, DBType.OSP);
		ops = in.openWritingSession(context, DBType.OPS);
		open = true;
	}

	public void close() {
		if (!open) {
			return;
		}
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
	}
	
	public int getCount(ActionContext context, Tuple tuple) {
		open(context);
		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();
		int len = encode(s, p, o);
		byte[] value = spo.get(key, len);
		if (value != null && value.length == 8) {
			return Utils.decodeInt(value, 4);
		}
		return 0;
	}

	/**
	 * Decreases the count of the triple in the DB. If the count goes to 0,
	 * removes it and returns true. Otherwise, return false.
	 * 
	 * @param tuple
	 *            the tuple to remove
	 * @return true iff the triple is removed from the DB
	 */
	public boolean decreaseAndRemoveTriple(ActionContext context, Tuple tuple, int count) {
		open(context);
		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();

		int len = encode(s, p, o);
		boolean removed = spo.decreaseOrRemove(key, len, count);

		len = encode(s, o, p);
		sop.decreaseOrRemove(key, len, count);

		len = encode(p, o, s);
		pos.decreaseOrRemove(key, len, count);

		len = encode(p, s, o);
		pso.decreaseOrRemove(key, len, count);

		len = encode(o, s, p);
		osp.decreaseOrRemove(key, len, count);

		len = encode(o, p, s);
		ops.decreaseOrRemove(key, len, count);

		return removed;
	}

	private int encode(long l1, long l2, long l3) {
		return in.encode(key, l1, l2, l3);
	}

}
