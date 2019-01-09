package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

	private static final long serialVersionUID = 1L;

	private Predicate predicate;
	private OpIterator child;

	/**
	 * Constructor accepts a predicate to apply and a child operator to read
	 * tuples to filter from.
	 *
	 * @param p     The predicate to filter tuples with
	 * @param child The child operator
	 */
	public Filter(Predicate p, OpIterator child) {
		// some code goes here
		this.predicate = p;
		this.child = child;
	}

	public Predicate getPredicate() {
		// some code goes here
		return this.predicate;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.child.getTupleDesc();
	}

	public void open() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		// some code goes here
		super.open();
		this.child.open();
	}

	public void close() {
		// some code goes here
		super.close();
		this.child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.child.rewind();
	}

	/**
	 * AbstractDbIterator.readNext implementation. Iterates over tuples from the
	 * child operator, applying the predicate to them and returning those that
	 * pass the predicate (i.e. for which the Predicate.filter() returns true.)
	 *
	 * @return The next tuple that passes the filter, or null if there are no
	 * more tuples
	 * @see Predicate#filter
	 */
	protected Tuple fetchNext() throws NoSuchElementException,
			TransactionAbortedException, DbException {
		// some code goes here
		if (this.child == null || this.predicate == null) {
			throw new DbException("Not open yet");
		}
		if (!this.child.hasNext()) {
			return null;
		}
		Tuple cur;
		while (child.hasNext()) {
			cur = this.child.next();
			if (this.predicate.filter(cur)) {
				return cur;
			}
		}
		return null;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		return new OpIterator[] { this.child };
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		if (children.length > 0) {
			this.child = children[0];
		}
	}

}
