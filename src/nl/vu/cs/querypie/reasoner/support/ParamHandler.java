package nl.vu.cs.querypie.reasoner.support;

/**
 * Singleton class containing all the configuration parameters for the
 * execution.
 */
public class ParamHandler {
	private static final ParamHandler instance = new ParamHandler();
	private boolean usingCount;
	private int lastStep;

	public static ParamHandler get() {
		return instance;
	}

	private ParamHandler() {
		usingCount = false;
		lastStep = 0;
	}

	public boolean isUsingCount() {
		return usingCount;
	}

	public void setUsingCount(boolean usingCount) {
		this.usingCount = usingCount;
	}

	public int getLastStep() {
		return lastStep;
	}

	public void setLastStep(int lastStep) {
		this.lastStep = lastStep;
	}

}
