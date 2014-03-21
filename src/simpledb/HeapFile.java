package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc tupleDesc;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */

	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		this.file = f;
		this.tupleDesc = td;
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
	 * you will need to generate this tableid somewhere ensure that each
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
		if (pid.getTableId() == this.getId()) {
			try {
				RandomAccessFile raFile = new RandomAccessFile(this.file, "r");
				// skip header
				int contentPointer = pid.pageNumber() * BufferPool.PAGE_SIZE;
				byte[] content = new byte[BufferPool.PAGE_SIZE];
				raFile.seek(contentPointer);
				raFile.read(content, 0, BufferPool.PAGE_SIZE);
				raFile.close();
				return new HeapPage((HeapPageId) pid, content);
			} catch (FileNotFoundException e) {
				System.out.println(e);
			} catch (IOException e) {
				System.out.println(e);
			}
		} else {
			throw new IllegalArgumentException(
					"PageID does not reference any Table via Catalog#getDbFile");
		}
		throw new IllegalArgumentException();
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for proj1
		if (page.getId().getTableId() == this.getId()) {
			try {
				byte[] content = new byte[BufferPool.PAGE_SIZE];
				RandomAccessFile raFile = new RandomAccessFile(this.file, "rw");
				int pageOffset = page.getId().pageNumber()
						* BufferPool.PAGE_SIZE;
				raFile.seek(pageOffset);
				raFile.write(content, 0, BufferPool.PAGE_SIZE);
				raFile.close();
			} catch (FileNotFoundException e) {
				System.out.println(e);
			} catch (IOException e) {
				System.out.println(e);
			}
		} else {
			throw new IllegalArgumentException(
					"PageID does not reference any Table via Catalog#getDbFile");
		}
		// throw new IllegalArgumentException();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		return (int) Math.floor(this.file.length() / BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// return null;
		// not necessary for proj1
		ArrayList<Page> pages = new ArrayList<Page>();

		if (!t.getTupleDesc().equals(this.getTupleDesc())) {
			throw new DbException("TupleDesc is mismatched!");
		} else {
			HeapPage page;
			boolean insertFinished = false;
			for (int i = 0; i < this.numPages(); i++) {
				HeapPageId pid = new HeapPageId(this.getId(), i);
				page = (HeapPage) (Database.getBufferPool().getPage(tid, pid,
						Permissions.READ_WRITE));
				try {
					page.insertTuple(t);
					pages.add(page);
					insertFinished = true;
					page.markDirty(true, tid);
					break;
				} catch (DbException e) {
					// System.out.println(e);
				}
			}
			if (insertFinished) {
				return pages;
			} else {
				// need Id to create a new page
				HeapPageId tempId = new HeapPageId(this.getId(),
						this.numPages());
				HeapPage tempPage = new HeapPage(tempId,
						HeapPage.createEmptyPageData());
				this.writePage(tempPage);
				HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(
						tid, tempId, Permissions.READ_WRITE);
				try {
					newPage.insertTuple(t);
					pages.add(newPage);
					newPage.markDirty(true, tid);
				} catch (DbException e) {
					throw e;
				}
				return pages;
			}
		}
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		// some code goes here
		// return null;
		// not necessary for proj1
		PageId pid = t.getRecordId().getPageId();
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
				Permissions.READ_WRITE);
		try {
			page.deleteTuple(t);
			return page;
		} catch (DbException e) {
			throw e;
		}
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		return new HeapFileIterator(tid);

	}

	private class HeapFileIterator implements DbFileIterator {
		/**
		 * add serialVerionUID
		 */
		private static final long serialVersionUID = 1L;
		private TransactionId tid;
		private int pageNum;
		private Iterator<Tuple> pageIterator;
		private boolean read;

		public HeapFileIterator(TransactionId tid) {
			this.tid = tid;
		}

		public void open() throws DbException, TransactionAbortedException {
			this.read = true;
			this.pageNum = 0;
			this.pageIterator = getTupleIteratorFromPage(0);

		}

		private Iterator<Tuple> getTupleIteratorFromPage(int pageNum)
				throws DbException, TransactionAbortedException {
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			// page retrieved from BufferPool
			BufferPool bp = Database.getBufferPool();
			PageId pageId = new HeapPageId(getId(), pageNum);
			Page page = bp.getPage(this.tid, pageId, Permissions.READ_ONLY);
			Iterator<Tuple> heapIterator = ((HeapPage) page).iterator();
			while (heapIterator.hasNext())
				tuples.add(heapIterator.next());
			return tuples.iterator();
		}

		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			if (this.read == false || this.pageIterator == null
					|| this.pageNum >= numPages())
				return false;
			if (this.pageIterator.hasNext())
				return true;
			else if (this.pageNum < numPages() - 1) {
				Iterator<Tuple> nextTuples = getTupleIteratorFromPage(this.pageNum + 1);
				return nextTuples.hasNext();
			}
			return false;
		}

		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			if (!this.read)
				throw new NoSuchElementException(
						"No more tuples exist on page number " + this.pageNum);
			if (this.pageIterator == null)
				throw new NoSuchElementException("The Page is null");
			if (this.pageIterator.hasNext())
				return this.pageIterator.next();
			if (this.pageNum < numPages() - 1) {
				this.pageIterator = getTupleIteratorFromPage(this.pageNum + 1);
				if (this.pageIterator.hasNext()) {
					this.pageNum++;
					return this.pageIterator.next();
				}
				throw new NoSuchElementException(
						"No more tuples exist on page number " + this.pageNum);
			}
			throw new NoSuchElementException(
					"No more tuples exist on page number " + this.pageNum);
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			this.close();
			this.open();

		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			pageIterator = null;

		}

	}
}
