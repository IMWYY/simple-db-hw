package simpledb;

import java.io.File;
import java.io.IOException;

public class QueryExample {

	public static void main(String[] args) throws IOException, TransactionAbortedException, DbException {
		//		SeqScan();
		JoinAndFilter();
	}

	static void JoinAndFilter() throws DbException, TransactionAbortedException, IOException {
		// init tuples
		Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
		String[] names = new String[] { "field0", "field1", "field2" };
		TupleDesc td = new TupleDesc(types, names);

		// add tables
		HeapFile file1 = new HeapFile(new File("some_data1.dat"), td);
		HeapFile file2 = new HeapFile(new File("some_data2.dat"), td);
		Database.getCatalog().addTable(file1, "table1");
		Database.getCatalog().addTable(file2, "table2");

		// construct operator
		TransactionId tid = new TransactionId();
		SeqScan scan1 = new SeqScan(tid, file1.getId());
		SeqScan scan2 = new SeqScan(tid, file2.getId());

		Filter filter = new Filter(new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)), scan1);
		JoinPredicate joinP = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
		Join join = new Join(joinP, filter, scan2);

		// iterator result
		join.open();
		while (join.hasNext()) {
			System.out.println(join.next().toString());
		}
		join.close();
		Database.getBufferPool().transactionComplete(tid);

	}

	static void SeqScan() throws DbException, TransactionAbortedException, IOException {
		// init tuple
		Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
		String[] fields = new String[] { "field0", "field1", "field2" };
		TupleDesc td = new TupleDesc(types, fields);

		// add table
		HeapFile heapFile = new HeapFile(new File("test.dat"), td);
		Database.getCatalog().addTable(heapFile, "test");

		// seqScan
		TransactionId tid = new TransactionId();
		SeqScan scan = new SeqScan(tid, heapFile.getId());

		scan.open();
		while (scan.hasNext()) {
			Tuple tup = scan.next();
			System.out.println(tup);
		}
		scan.close();
		Database.getBufferPool().transactionComplete(tid);
	}
}
