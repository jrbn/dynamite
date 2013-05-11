package nl.vu.cs.dynamite.storage;

public interface WritingSession {

	public int SUCCESS = 0;
	public int ALREADY_EXISTING = 1;
	public int ERROR = 2;

	public void close();

	public boolean decreaseOrRemove(byte[] key, int keyLen, int count);

	public int remove(byte[] key, int keyLen);

	public int write(byte[] key, int keyLen, byte[] value) throws Exception;

	public int writeWithCount(byte[] key, int keyLen, byte[] value, int c,
			boolean override) throws Exception;

	public int writeKey(byte[] key, int keyLen);

	// J: instead of boolean returns the original step. Needed by the algorithm
	public int removeIfStepNonZero(byte[] key, int len);

	public byte[] get(byte[] key, int len);
}
