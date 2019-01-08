package simpledb;

import java.io.File;

public class test {

	public static void main(String[] args) {
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

		try {
			scan.open();
			while (scan.hasNext()) {
				Tuple tup = scan.next();
				System.out.println(tup);
			}
			scan.close();
			Database.getBufferPool().transactionComplete(tid);
		} catch (Exception e) {
			System.out.println("Exception : " + e);
		}

	}
}
