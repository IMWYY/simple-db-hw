package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	private static final long serialVersionUID = 1L;
	private TupleDesc tupleDesc;
	private List<Field> fields;
	private RecordId recordId;

	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td the schema of this tuple. It must be a valid TupleDesc
	 *           instance with at least one field.
	 */
	public Tuple(TupleDesc td) {
		// some code goes here
		this.tupleDesc = td;
		this.fields = new ArrayList<>(td.numFields());
		for (int i = 0; i < td.numFields(); ++i) {
			this.fields.add(null);
		}
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.tupleDesc;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 * be null.
	 */
	public RecordId getRecordId() {
		// some code goes here
		return this.recordId;
	}

	/**
	 * Set the RecordId information for this tuple.
	 *
	 * @param rid the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		// some code goes here
		this.recordId = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i index of the field to change. It must be a valid index.
	 * @param f new value for the field.
	 */
	public void setField(int i, Field f) {
		// some code goes here
		this.fields.set(i, f);
	}

	/**
	 * @param i field index to return. Must be a valid index.
	 * @return the value of the ith field, or null if it has not been set.
	 */
	public Field getField(int i) {
		// some code goes here
		return this.fields.get(i);
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 * <p>
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN
	 * <p>
	 * where \t is any whitespace (except a newline)
	 */
	public String toString() {
		// some code goes here
		StringBuilder sb = new StringBuilder();
		if (fields.size() > 0) {
			sb.append(fields.get(0).toString());
		}
		for (int i = 1; i < this.fields.size(); ++i) {
			sb.append(" ").append(this.fields.get(i).toString());
		}
		return sb.toString();
	}

	/**
	 * @return An iterator which iterates over all the fields of this tuple
	 */
	public Iterator<Field> fields() {
		// some code goes here
		return fields.iterator();
	}

	/**
	 * reset the TupleDesc of this tuple (only affecting the TupleDesc)
	 */
	public void resetTupleDesc(TupleDesc td) {
		// some code goes here
		this.tupleDesc = td;
		this.fields = new ArrayList<>(td.numFields());
	}
}
