package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

	static final int IOCOSTPERPAGE = 1000;
	/**
	 * Number of bins for the histogram. Feel free to increase this value over
	 * 100, though our tests assume that you have at least 100 bins in your
	 * histograms.
	 */
	static final int NUM_HIST_BINS = 100;
	private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

	public static TableStats getTableStats(String tablename) {
		return statsMap.get(tablename);
	}

	public static void setTableStats(String tablename, TableStats stats) {
		statsMap.put(tablename, stats);
	}

	public static Map<String, TableStats> getStatsMap() {
		return statsMap;
	}

	public static void setStatsMap(HashMap<String, TableStats> s) {
		try {
			java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
			statsMapF.setAccessible(true);
			statsMapF.set(null, s);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}

	}

	public static void computeStatistics() {
		Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

		System.out.println("Computing table stats.");
		while (tableIt.hasNext()) {
			int tableid = tableIt.next();
			TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
			setTableStats(Database.getCatalog().getTableName(tableid), s);
		}
		System.out.println("Done.");
	}

	private int ioCostPerPage;
	private Object[] fieldIndex2Hist;
	private TupleDesc td;
	private int numTuples;
	private DbFile dbFile;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * column of a table
	 *
	 * @param tableid       The table over which to compute statistics
	 * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
	 *                      sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		// For this function, you'll have to get the DbFile for the table in question,
		// then scan through its tuples and calculate the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything in a single scan of the table.
		// some code goes here
		this.ioCostPerPage = ioCostPerPage;
		this.initTableStats(tableid);
	}

	private void initTableStats(int tableId) {
		TransactionId tid = new TransactionId();
		this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
		DbFileIterator iterator = dbFile.iterator(tid);
		this.td = dbFile.getTupleDesc();

		this.fieldIndex2Hist = new Object[td.numFields()];

		int[] minList = new int[td.numFields()];
		int[] maxList = new int[td.numFields()];
		Arrays.fill(minList, Integer.MAX_VALUE);
		Arrays.fill(maxList, Integer.MIN_VALUE);
		try {
			iterator.open();
			while (iterator.hasNext()) {
				Tuple t = iterator.next();
				this.numTuples++;
				for (int i = 0; i < td.numFields(); ++i) {
					if (td.getFieldType(i) == Type.INT_TYPE) {
						int v = ((IntField) t.getField(i)).getValue();
						minList[i] = Math.min(minList[i], v);
						maxList[i] = Math.max(maxList[i], v);
					}
				}
			}
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
			iterator.close();
		}

		for (int i = 0; i < td.numFields(); ++i) {
			if (td.getFieldType(i) == Type.INT_TYPE) {
				this.fieldIndex2Hist[i] = new IntHistogram(NUM_HIST_BINS, minList[i], maxList[i]);
			} else if (td.getFieldType(i) == Type.STRING_TYPE) {
				this.fieldIndex2Hist[i] = new StringHistogram(NUM_HIST_BINS);
			} else {
				throw new IllegalArgumentException();
			}
		}

		try {
			iterator.rewind();
			iterator.open();
			while (iterator.hasNext()) {
				Tuple t = iterator.next();
				for (int i = 0; i < td.numFields(); ++i) {
					if (td.getFieldType(i) == Type.INT_TYPE) {
						((IntHistogram) this.fieldIndex2Hist[i]).addValue(((IntField) t.getField(i)).getValue());
					} else if (td.getFieldType(i) == Type.STRING_TYPE) {
						((StringHistogram) this.fieldIndex2Hist[i]).addValue(((StringField) t.getField(i)).getValue());
					} else {
						throw new IllegalArgumentException();
					}
				}
			}
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		} finally {
			iterator.close();
		}
	}

	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * <p>
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 *
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		// some code goes here
		if (this.dbFile instanceof HeapFile) {
			return ((HeapFile) this.dbFile).numPages() * ioCostPerPage;
		} else if (this.dbFile instanceof BTreeFile) {
			return ((BTreeFile) this.dbFile).numPages() * ioCostPerPage;
		}
		throw new IllegalArgumentException();
	}

	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 *
	 * @param selectivityFactor The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 * selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		// some code goes here
		return (int) Math.ceil(totalTuples() * selectivityFactor);
	}

	/**
	 * The average selectivity of the field under op.
	 *
	 * @param field the index of the field
	 * @param op    the operator in the predicate
	 *              The semantic of the method is that, given the table, and then given a
	 *              tuple, of which we do not know the value of the field, return the
	 *              expected selectivity. You may estimate this value from the histograms.
	 */
	public double avgSelectivity(int field, Predicate.Op op) {
		// some code goes here
		if (field < 0 || field >= this.fieldIndex2Hist.length) {
			throw new IllegalArgumentException();
		}
		if (this.td.getFieldType(field) == Type.INT_TYPE) {
			return ((IntHistogram) this.fieldIndex2Hist[field]).avgSelectivity();
		} else {
			return ((StringHistogram) this.fieldIndex2Hist[field]).avgSelectivity();
		}
	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 *
	 * @param field    The field over which the predicate ranges
	 * @param op       The logical operation in the predicate
	 * @param constant The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 * predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		// some code goes here
		if (field < 0 || field >= this.fieldIndex2Hist.length) {
			throw new IllegalArgumentException();
		}
		if (this.td.getFieldType(field) == Type.INT_TYPE) {
			if (!(constant instanceof IntField)) {
				throw new IllegalArgumentException();
			}
			Debug.log("TableStats#estimateSelectivity:" + constant.toString());
			return ((IntHistogram) this.fieldIndex2Hist[field])
					.estimateSelectivity(op, ((IntField) constant).getValue());
		} else {
			if (!(constant instanceof StringField)) {
				throw new IllegalArgumentException();
			}
			return ((StringHistogram) this.fieldIndex2Hist[field])
					.estimateSelectivity(op, ((StringField) constant).getValue());
		}
	}

	/**
	 * return the total number of tuples in this table
	 */
	public int totalTuples() {
		// some code goes here
		return this.numTuples;
	}

}
