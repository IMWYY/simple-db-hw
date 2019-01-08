package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc tupleDesc;
	private int numOfPage;

	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f the file that stores the on-disk backing store for this heap
	 *          file.
	 */
	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		this.file = f;
		this.tupleDesc = td;
		this.numOfPage = (int) f.length() / BufferPool.getPageSize();
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		return this.file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere to ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// some code goes here
		return this.file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return this.tupleDesc;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// some code goes here
		Page page = null;
		byte[] data = new byte[BufferPool.getPageSize()];
		int offset = pid.getPageNumber() * BufferPool.getPageSize();

		try (RandomAccessFile rf = new RandomAccessFile(getFile(), "r")) {
			rf.seek(offset);
			rf.read(data, 0, BufferPool.getPageSize());
			page = new HeapPage((HeapPageId) pid, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return page;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		return numOfPage;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileIterator(this, tid);
	}

	private class HeapFileIterator implements DbFileIterator {

		private HeapFile heapFile;
		private TransactionId tid;
		private Iterator<Tuple> curPageTuples;
		private int curPageNo;

		public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
			this.heapFile = heapFile;
			this.tid = tid;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.curPageNo = 0;
			this.curPageTuples = getNextPageTuples(curPageNo);
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// not initialized yet
			if (curPageTuples == null) {
				return false;
			}
			if (curPageTuples.hasNext()) {
				return true;
			}

			// get next non empty page and return whether has next
			while (curPageNo < heapFile.numPages() - 1 && !curPageTuples.hasNext()) {
				this.curPageNo++;
				this.curPageTuples = getNextPageTuples(this.curPageNo);
			}

			if (curPageNo >= heapFile.numPages()) {
				return false;
			}
			return curPageTuples.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!hasNext())
				throw new NoSuchElementException();
			return curPageTuples.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			open();
		}

		@Override
		public void close() {
			this.curPageNo = 0;
			this.curPageTuples = null;
		}

		private Iterator<Tuple> getNextPageTuples(int pageNo) throws TransactionAbortedException, DbException {
			PageId pid = new HeapPageId(heapFile.getId(), pageNo);
			return ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
		}
	}

}

