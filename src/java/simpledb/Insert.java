package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId tid;
	private OpIterator child;
	private int tableId;
	private TupleDesc td;
	private Tuple resultTuple;

	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableId The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we are to
	 *                     insert.
	 */
	public Insert(TransactionId t, OpIterator child, int tableId)
			throws DbException {
		// some code goes here
		this.tid = t;
		this.child = child;
		this.tableId = tableId;
		this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.td;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		super.open();
		this.child.open();

		int affestRow = 0;
		while(this.child.hasNext()) {
			try {
				Database.getBufferPool().insertTuple(this.tid, this.tableId, this.child.next());
				affestRow ++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.resultTuple = new Tuple(this.td);
		this.resultTuple.setField(0, new IntField(affestRow));
	}

	public void close() {
		// some code goes here
		super.close();
		this.child.close();
		this.resultTuple = null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.child.open();
		this.resultTuple = null;
	}

	/**
	 * Inserts tuples read from child into the tableId specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or
	 * null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		Tuple res = this.resultTuple;
		if (this.resultTuple != null) {
			this.resultTuple = null;
		}
		return res;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
		return new OpIterator[] { this, child };
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		if (children.length > 0) {
			this.child = children[0];
		}
	}
}
