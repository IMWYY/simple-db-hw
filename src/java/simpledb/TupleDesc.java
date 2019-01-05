package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private static final long serialVersionUID = 1L;
	private List<TDItem> tdItems;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr  array specifying the number of and types of fields in this
	 *                TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// some code goes here
		this.tdItems = new ArrayList<>();
		for (int i = 0; i < typeAr.length; ++i) {
			this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
		}
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr array specifying the number of and types of fields in this
	 *               TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// some code goes here
		this.tdItems = new ArrayList<>();
		for (Type aTypeAr : typeAr) {
			this.tdItems.add(new TDItem(aTypeAr, null));
		}
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 *
	 * @param td1 The TupleDesc with the first fields of the new TupleDesc
	 * @param td2 The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		// some code goes here
		Type[] types = new Type[td1.numFields() + td2.numFields()];
		String[] names = new String[td1.numFields() + td2.numFields()];
		int index = 0;
		Iterator<TDItem> it1 = td1.iterator();
		Iterator<TDItem> it2 = td2.iterator();
		while (it1.hasNext()) {
			TDItem t = it1.next();
			types[index] = t.fieldType;
			names[index] = t.fieldName;
			index++;
		}
		while (it2.hasNext()) {
			TDItem t = it2.next();
			types[index] = t.fieldType;
			names[index] = t.fieldName;
			index++;
		}
		return new TupleDesc(types, names);
	}

	/**
	 * @return An iterator which iterates over all the field TDItems
	 * that are included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		// some code goes here
		return tdItems.iterator();
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		return this.tdItems.size();
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// some code goes here
		if (i >= this.tdItems.size()) {
			throw new NoSuchElementException();
		}
		return this.tdItems.get(i).fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i The index of the field to get the type of. It must be a valid
	 *          index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		// some code goes here
		if (i >= this.tdItems.size()) {
			throw new NoSuchElementException();
		}
		return this.tdItems.get(i).fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// some code goes here
		for (int i = 0; i < this.tdItems.size(); ++i) {
			if (Objects.equals(name, this.tdItems.get(i).fieldName)) {
				return i;
			}
		}
		throw new NoSuchElementException();
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 * Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// some code goes here
		int res = 0;
		for (TDItem tdItem : this.tdItems) {
			res += tdItem.fieldType.getLen();
		}
		return res;
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they have the same number of items
	 * and if the i-th type in this TupleDesc is equal to the i-th type in o
	 * for every i.
	 *
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */

	public boolean equals(Object o) {
		// some code goes here
		if (!(o instanceof TupleDesc)) {
			return false;
		}
		TupleDesc oth = (TupleDesc) o;
		if (oth.tdItems.size() != this.tdItems.size()) {
			return false;
		}
		for (int i = 0; i < this.tdItems.size(); ++i) {
			TDItem t1 = this.tdItems.get(i), t2 = oth.tdItems.get(i);
			if (t1.fieldType != t2.fieldType || !Objects.equals(t1.fieldName, t2.fieldName)) {
				return false;
			}
		}
		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		int res = 17;
		for (TDItem t : this.tdItems) {
			res += t.fieldType.getLen() * 7 + 23 * t.fieldName.hashCode();
		}
		return res;
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 *
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		StringBuilder stringBuilder = new StringBuilder();
		if (this.tdItems.size() > 0) {
			stringBuilder.append(this.tdItems.get(0).toString());
		}
		for (int i = 1; i < this.tdItems.size(); ++i) {
			stringBuilder.append(",").append(this.tdItems.get(i).toString());
		}
		return stringBuilder.toString();
	}

	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		public final Type fieldType;

		/**
		 * The name of the field
		 */
		public final String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}
}

