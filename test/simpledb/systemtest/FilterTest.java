package simpledb.systemtest;

import simpledb.DbException;
import simpledb.Filter;
import simpledb.HeapFile;
import simpledb.Predicate;
import simpledb.SeqScan;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class FilterTest extends FilterBase {
	/**
	 * Make test compatible with older version of ant.
	 */
	public static junit.framework.Test suite() {
		return new junit.framework.JUnit4TestAdapter(FilterTest.class);
	}

	@Override
	protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
			throws DbException, TransactionAbortedException, IOException {
		SeqScan ss = new SeqScan(tid, table.getId(), "");
		Filter filter = new Filter(predicate, ss);
		filter.open();

		int resultCount = 0;
		while (filter.hasNext()) {
			assertNotNull(filter.next());
			resultCount += 1;
		}

		filter.close();
		return resultCount;
	}
}
