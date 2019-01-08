package simpledb;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

	private static final long serialVersionUID = 1L;

	private TransactionId tid;
	private int tableId;
	private String tableAlias;
	private DbFileIterator dbFileIterator;
	private TupleDesc tupleDesc;

	/**
	 * Creates a sequential scan over the specified table as a part of the
	 * specified transaction.
	 *
	 * @param tid        The transaction this scan is running as a part of.
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the returned
	 *                   tupleDesc should have fields with name tableAlias.fieldName
	 *                   (note: this class is not responsible for handling a case where
	 *                   tableAlias or fieldName are null. It shouldn't crash if they
	 *                   are, but the resulting name can be null.fieldName,
	 *                   tableAlias.null, or null.null).
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias) {
		// some code goes here
		this.tid = tid;
		this.tableId = tableid;
		this.tableAlias = tableAlias;
		this.tupleDesc = initTupleDesc(tableid, tableAlias);
	}

	public SeqScan(TransactionId tid, int tableId) {
		this(tid, tableId, Database.getCatalog().getTableName(tableId));
	}

	private TupleDesc initTupleDesc(int tableid, String alias) {
		TupleDesc desc = Database.getCatalog().getTupleDesc(tableid);
		Type[] types = new Type[desc.numFields()];
		String[] names = new String[desc.numFields()];
		for (int i = 0; i < desc.numFields(); ++i) {
			types[i] = desc.getFieldType(i);
			names[i] = (alias == null ? "null" : alias) + "." +
					(desc.getFieldName(i) == null ? "null" : desc.getFieldName(i));
		}
		return new TupleDesc(types, names);
	}

	/**
	 * @return return the table name of the table the operator scans. This should
	 * be the actual name of the table in the catalog of the database
	 */
	public String getTableName() {
		return Database.getCatalog().getTableName(this.tableId);
	}

	/**
	 * @return Return the alias of the table this operator scans.
	 */
	public String getAlias() {
		// some code goes here
		return this.tableAlias;
	}

	/**
	 * Reset the tableid, and tableAlias of this operator.
	 *
	 * @param tableid    the table to scan.
	 * @param tableAlias the alias of this table (needed by the parser); the returned
	 *                   tupleDesc should have fields with name tableAlias.fieldName
	 *                   (note: this class is not responsible for handling a case where
	 *                   tableAlias or fieldName are null. It shouldn't crash if they
	 *                   are, but the resulting name can be null.fieldName,
	 *                   tableAlias.null, or null.null).
	 */
	public void reset(int tableid, String tableAlias) {
		// some code goes here
		this.tableId = tableid;
		this.tableAlias = tableAlias;
		this.tupleDesc = initTupleDesc(tableid, tableAlias);
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		this.dbFileIterator = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.tid);
		this.dbFileIterator.open();
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile,
	 * prefixed with the tableAlias string from the constructor. This prefix
	 * becomes useful when joining tables containing a field(s) with the same
	 * name.  The alias and name should be separated with a "." character
	 * (e.g., "alias.fieldName").
	 *
	 * @return the TupleDesc with field names from the underlying HeapFile,
	 * prefixed with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.tupleDesc;
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (this.dbFileIterator == null) {
			throw new IllegalStateException();
		}
		return this.dbFileIterator.hasNext();
	}

	public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		if (this.dbFileIterator == null) {
			throw new IllegalStateException();
		}
		return this.dbFileIterator.next();
	}

	public void close() {
		// some code goes here
		if (this.dbFileIterator != null) {
			this.dbFileIterator.close();
		}
	}

	public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		if (this.dbFileIterator == null) {
			throw new IllegalStateException();
		}

		this.dbFileIterator.rewind();
	}
}
