package nl.vu.cs.dynamite.storage;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.Tuple;

public interface BTreeInterface {

	public WritingSession openWritingSession(ActionContext context, DBType type) throws Exception;

	public int encode(byte[] triple, long l1, long l2, long l3);

	public void remove(Tuple tuple);

	public void decreaseOrRemove(Tuple tuple, int count);
	
	public void close();
}
