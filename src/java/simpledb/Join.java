package simpledb;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

	private static final long serialVersionUID = 1L;

	private JoinPredicate joinPredicate;
	private OpIterator child1;
	private OpIterator child2;

	private TupleDesc tupleDesc;

	private Tuple curOuterTuple;

	/**
	 * Constructor. Accepts two children to join and the predicate to join them
	 * on
	 *
	 * @param p      The predicate to use to join the children
	 * @param child1 Iterator for the left(outer) relation to join
	 * @param child2 Iterator for the right(inner) relation to join
	 */
	public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
		// some code goes here
		this.joinPredicate = p;
		this.child1 = child1;
		this.child2 = child2;
		this.tupleDesc = TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
	}

	public JoinPredicate getJoinPredicate() {
		// some code goes here
		return this.joinPredicate;
	}

	/**
	 * @return the field name of join field1. Should be quantified by
	 * alias or table name.
	 */
	public String getJoinField1Name() {
		// some code goes here
		return this.child1.getTupleDesc().getFieldName(this.joinPredicate.getField1());
	}

	/**
	 * @return the field name of join field2. Should be quantified by
	 * alias or table name.
	 */
	public String getJoinField2Name() {
		// some code goes here
		return this.child2.getTupleDesc().getFieldName(this.joinPredicate.getField2());
	}

	/**
	 * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
	 * implementation logic.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.tupleDesc;
	}

	public void open() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		// some code goes here
		super.open();
		this.child1.open();
		this.child2.open();
	}

	public void close() {
		// some code goes here
		super.close();
		this.child1.close();
		this.child2.close();
		this.curOuterTuple = null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.child1.rewind();
		this.child2.rewind();
		this.curOuterTuple = null;
	}

	/**
	 * Returns the next tuple generated by the join, or null if there are no
	 * more tuples. Logically, this is the next tuple in r1 cross r2 that
	 * satisfies the join predicate. There are many possible implementations;
	 * the simplest is a nested loops join.
	 * <p>
	 * Note that the tuples returned from this particular implementation of Join
	 * are simply the concatenation of joining tuples from the left and right
	 * relation. Therefore, if an equality predicate is used there will be two
	 * copies of the join attribute in the results. (Removing such duplicate
	 * columns can be done with an additional projection operator if needed.)
	 * <p>
	 * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
	 * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
	 *
	 * @return The next matching tuple.
	 * @see JoinPredicate#filter
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (curOuterTuple == null) {
			if (!this.child1.hasNext()) {
				return null;
			}
			curOuterTuple = this.child1.next();
		}

		while (curOuterTuple != null) {
			while (this.child2.hasNext()) {
				Tuple t = this.child2.next();
				if (this.joinPredicate.filter(curOuterTuple, t)) {
					Tuple res = new Tuple(this.tupleDesc);
					for (int i = 0; i < this.child1.getTupleDesc().numFields(); ++i) {
						res.setField(i, curOuterTuple.getField(i));
					}
					for (int i = 0; i < this.child2.getTupleDesc().numFields(); ++i) {
						res.setField(i + this.child1.getTupleDesc().numFields(), t.getField(i));
					}
					return res;
				}
			}

			if (!this.child1.hasNext()) {
				return null;
			}
			curOuterTuple = this.child1.next();
			this.child2.rewind();
		}

		return null;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		return new OpIterator[] { this.child1, this.child2 };
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		if (children.length > 1) {
			this.child1 = children[0];
			this.child2 = children[1];
			this.tupleDesc = TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
		}
	}

}
