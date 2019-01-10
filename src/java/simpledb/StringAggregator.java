package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int gbField;
	private Type gbFieldType;
	private int aField;
	private Op op;
	private TupleDesc td;

	// 如果NO_GROUPING key设为IntField(0)
	private Map<Field, Integer> aggResult;

	/**
	 * Aggregate constructor
	 *
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what, TupleDesc td) {
		// some code goes here
		if (what != Op.COUNT) {
			throw new IllegalArgumentException();
		}
		this.gbField = gbfield;
		this.gbFieldType = gbfieldtype;
		this.aField = afield;
		this.op = what;
		this.td = td;
		this.aggResult = new HashMap<>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		Field groupField, aggField = tup.getField(this.aField);
		if (this.gbField == NO_GROUPING) {
			groupField = new IntField(0);
		} else {
			groupField = tup.getField(this.gbField);
		}

		if (!(aggField instanceof StringField)) {
			throw new IllegalStateException();
		}

		int curCount = this.aggResult.getOrDefault(groupField, 0);
		this.aggResult.put(groupField, curCount + 1);
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal,
	 * aggregateVal) if using group, or a single (aggregateVal) if no
	 * grouping. The aggregateVal is determined by the type of
	 * aggregate specified in the constructor.
	 */
	public OpIterator iterator() {
		// some code goes here
		ArrayList<Tuple> tuples = new ArrayList<>(this.aggResult.size());
		for (Map.Entry<Field, Integer> entry : this.aggResult.entrySet()) {
			Tuple t = new Tuple(td);
			if (this.gbField == NO_GROUPING) {
				t.setField(0, new IntField(entry.getValue()));
			} else {
				t.setField(0, entry.getKey());
				t.setField(1, new IntField(entry.getValue()));
			}
			tuples.add(t);
		}
		return new TupleIterator(this.td, tuples);
	}

}
