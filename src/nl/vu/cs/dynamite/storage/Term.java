package nl.vu.cs.dynamite.storage;

public class Term {
	private String name;
	private long value;

	public Term(long value) {
		this.value = value;
	}

	public Term() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public void copyTo(Term term) {
		term.name = name;
		term.value = value;
	}

	@Override
	public String toString() {
		return value + "(" + name + ")";
	}
}
