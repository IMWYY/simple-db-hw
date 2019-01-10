package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int gbField;
	private Type gbFieldType;
	private int aField;
	private Op op;
	private TupleDesc td;

	// 如果NO_GROUPING key设为IntField(0)
	private Map<Field, Integer> aggResult;

	// 每个group中的tuple数量 用于计算avg
	private Map<Field, Integer> eachGroupCount;
	// 求平均需要用到 因为整数除法有精度损失 所以不能用res=(curVal*curCount+newVal)/(curCount+1)的方法
	private Map<Field, Integer> eachGroupSum;

	/**
	 * Aggregate constructor
	 *
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
	 *                    if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what, TupleDesc td) {
		// some code goes here
		this.gbField = gbfield;
		this.gbFieldType = gbfieldtype;
		this.aField = afield;
		this.op = what;
		this.td = td;
		this.aggResult = new HashMap<>();
		this.eachGroupCount = new HashMap<>();
		this.eachGroupSum = new HashMap<>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
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
		if (!(aggField instanceof IntField)) {
			throw new IllegalStateException();
		}
		int newVal = getNewAggVal(groupField, ((IntField) aggField).getValue());
		this.aggResult.put(groupField, newVal);
	}

	/**
	 * 获取添加元素后的新值
	 * 需要注意的是第一次添加的时候的初始值
	 * 获取min初始值应该是MAX 获取max初始值应该是MIN
	 *
	 * @param groupField groupBy的Field
	 * @param newVal     新添加的值
	 * @return 计算后的新值
	 */
	private int getNewAggVal(Field groupField, int newVal) {
		int res = 0, curVal;
		switch (this.op) {
		case AVG:
			int curSum = this.eachGroupSum.getOrDefault(groupField, 0);
			int curCount = this.eachGroupCount.getOrDefault(groupField, 0);
			this.eachGroupCount.put(groupField, curCount + 1);
			this.eachGroupSum.put(groupField, curSum + newVal);
			res = (curSum + newVal) / (curCount + 1);
			break;
		case MAX:
			curVal = this.aggResult.getOrDefault(groupField, Integer.MIN_VALUE);
			res = Math.max(curVal, newVal);
			break;
		case MIN:
			curVal = this.aggResult.getOrDefault(groupField, Integer.MAX_VALUE);
			res = Math.min(curVal, newVal);
			break;
		case SUM:
			curVal = this.aggResult.getOrDefault(groupField, 0);
			res = curVal + newVal;
			break;
		case COUNT:
			curVal = this.aggResult.getOrDefault(groupField, 0);
			res = curVal + 1;
			break;
		case SC_AVG:
			break;
		case SUM_COUNT:
			break;
		}
		return res;
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
	 * if using group, or a single (aggregateVal) if no grouping. The
	 * aggregateVal is determined by the type of aggregate specified in
	 * the constructor.
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
