package nl.vu.cs.dynamite.io;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.dynamite.ReasoningContext;
import nl.vu.cs.dynamite.storage.BTreeInterface;
import nl.vu.cs.dynamite.storage.DBType;
import nl.vu.cs.dynamite.storage.WritingSession;

public class DBHandler {
	public String DBHandlerKey = "__DB_HANLDER_KEY";

	private BTreeInterface in;
	private WritingSession spo, sop, pos, pso, osp, ops;
	private final byte[] key = new byte[24];
	private boolean open;
	private int count;

	public DBHandler() {
		open = false;
	}

	public synchronized void open(ActionContext context) throws Exception {
		count++;
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

	public synchronized void close() {
		;
		if (!open) {
			return;
		}
		count--;
		if (count > 0) {
			return;
		}
		spo.close();
		sop.close();
		ops.close();
		osp.close();
		pos.close();
		pso.close();
		open = false;
	}

	public int getCount(ActionContext context, Tuple tuple) throws Exception {
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
	 * @throws Exception
	 */
	public boolean decreaseAndRemoveTriple(ActionContext context, Tuple tuple,
			int count) throws Exception {
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

	public boolean remove(ActionContext context, Tuple tuple) throws Exception {
		open(context);
		long s = ((TLong) tuple.get(0)).getValue();
		long p = ((TLong) tuple.get(1)).getValue();
		long o = ((TLong) tuple.get(2)).getValue();

		int len = encode(s, p, o);
		spo.remove(key, len);

		len = encode(s, o, p);
		sop.remove(key, len);

		len = encode(p, o, s);
		pos.remove(key, len);

		len = encode(p, s, o);
		pso.remove(key, len);

		len = encode(o, s, p);
		osp.remove(key, len);

		len = encode(o, p, s);
		ops.remove(key, len);

		return true;
	}

	private int encode(long l1, long l2, long l3) {
		return in.encode(key, l1, l2, l3);
	}

}
