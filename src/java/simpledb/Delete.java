package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId tid;
	private OpIterator child;
	private TupleDesc td;
	private Tuple resultTuple;

	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 *
	 * @param t     The transaction this delete runs in
	 * @param child The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, OpIterator child) {
		// some code goes here
		this.tid = t;
		this.child = child;
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
		int affectRows = 0;
		while (this.child.hasNext()) {
			try {
				Database.getBufferPool().deleteTuple(this.tid, this.child.next());
				affectRows ++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.resultTuple = new Tuple(this.td);
		this.resultTuple.setField(0, new IntField(affectRows));
	}

	public void close() {
		// some code goes here
		super.close();
		this.child.close();
		this.resultTuple = null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		this.child.rewind();
		this.resultTuple = null;
	}

	/**
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 *
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
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
		return new OpIterator[]{this.child};
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
		if (children.length > 0) {
			this.child = children[0];
		}
	}

}
