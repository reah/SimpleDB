package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {

    private static HashMap<PageId, Page> pages;
    private static int maxPages;

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        BufferPool.pages = new HashMap<PageId, Page>();
        BufferPool.maxPages = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int tableId = pid.getTableId();
        DbFile file = Database.getCatalog().getDbFile(tableId);
    	//Page temp = file.readPage(pid);
        Page temp = null;

        if(BufferPool.pages.containsKey(pid)){
        	//set time for LRU of currently being accessed heapPage to system.currentTimeMillis()
        	((HeapPage)BufferPool.pages.get(pid)).lastUsed = System.currentTimeMillis();
        	return BufferPool.pages.get(pid);
        }
    	else if(BufferPool.pages.get(pid) == null && (BufferPool.pages.size() < BufferPool.maxPages)) {
    		temp = file.readPage(pid);
            ((HeapPage)temp).lastUsed = System.currentTimeMillis();
    		BufferPool.pages.put(pid, temp);
    		return temp;
    	}
    	else {
    		this.evictPage();
            temp = file.readPage(pid);
    		((HeapPage)temp).lastUsed = System.currentTimeMillis();
    		BufferPool.pages.put(pid, temp);
    		return temp;
    	}
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	HeapFile file = (HeapFile)Database.getCatalog().getDbFile(tableId);
    	//Inserts the specified tuple to the file on behalf of transaction. 
    	ArrayList<Page> list = file.insertTuple(tid, t);
    	// get first page
    	Page p = list.get(0);
    	BufferPool.pages.put(p.getId(), p);    
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
    	//rid = <pageno, slotno> / <pageId, slotno>
    	PageId pid = t.getRecordId().getPageId();
    	HeapFile file = (HeapFile)Database.getCatalog().getDbFile(pid.getTableId());
    	Page p = file.deleteTuple(tid, t);
    	p.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
    	for(Page p : BufferPool.pages.values()) {
    		//check TransactionId
    		if (p.isDirty() != null) {
    			flushPage(p.getId());
    		}
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
    	Page p = BufferPool.pages.get(pid);
    	Catalog c = Database.getCatalog();
    	DbFile file = c.getDbFile(p.getId().getTableId());
    	p.markDirty(false, null);
    	//write flushed page to disk. In this project no need for DiskManager, OS does dirty work. 
    	file.writePage(p);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    	long minT = System.currentTimeMillis();
    	HeapPage LRUpage = null;
    	for(Page p : BufferPool.pages.values()) {
    		if (((HeapPage)p).lastUsed <= minT) {
    			minT = ((HeapPage)p).lastUsed;
    			LRUpage = (HeapPage)p;
    		}
    	}
		try {
			flushPage(LRUpage.getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	BufferPool.pages.remove(LRUpage.getId());
    }

}
