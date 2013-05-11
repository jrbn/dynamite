package nl.vu.cs.dynamite.storage;

/**
 * A DBType represents the type of index used inside the db.
 * T2N and N2T are dictionaries, text-to-number and number-to-text.
 * The others are Subject-Predicate-Object, et cetera.
 */
public enum DBType {
	SPO, SOP, PSO, POS, OSP, OPS, T2N, N2T
}
