package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;

    private ConcurrentHashMap<PageId,LinkedNode> bufferpool;
    //LinkNode is double head linklist and store the <pageid,page> map
    private static class LinkedNode {
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;
        public LinkedNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
    }
    /*
    LRU:
        当BufferPool中的page被访问时，将该pageId对应的LinkedNode移动到链表的头部，
        当有page需要放置到BufferPool中，且BufferPool的容量已经满时，则将最近最少使用的page淘汰，
        即链表的最后一个节点，然后将该page放置到BufferPool中。
    */
    //LTU implement method
    LinkedNode head, tail;
    //if one page is received in the first time, the linkednode would be placed to the head
    private void addToHead(LinkedNode node)
    {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }
    //remove the page from the bufferpool
    private void remove(LinkedNode node )
    {
        node.next.prev = node.prev;
        node.prev.next = node.next;
    }
    //if one page in the bufferpool is received, move it to the head
    private void moveToHead(LinkedNode node)
    {
        remove(node);
        addToHead(node);
    }
    //remove the last page in the list
    private LinkedNode removeTail()
    {
        LinkedNode node = tail.prev;
        remove(node);

        return node;
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        bufferpool = new ConcurrentHashMap<>(numPages);
        head = new LinkedNode(new HeapPageId(-1, -1), null);
        tail = new LinkedNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
            //if bufferpool do not have the page
            if (!bufferpool.containsKey(pid))
            {
                //get the page
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page page = dbFile.readPage(pid);
                if ( bufferpool.size() > numPages )
                {
                    //use the evict policy
                    evictPage();
                }
                LinkedNode node = new LinkedNode(pid, page);
                bufferpool.put(pid, node);
                addToHead(node);
            }
            //once the page is received, it should be moved to head
            moveToHead(bufferpool.get(pid));

            return bufferpool.get(pid).page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // 获取 数据库文件 DBfile
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = heapFile.insertTuple(tid, t);
        // 将页面刷新到缓存中
        updateBufferPool(pages, tid);

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = heapFile.deleteTuple(tid, t);
        updateBufferPool(pages,tid);
    }

    private void updateBufferPool(List<Page> pagelist, TransactionId tid)
            throws DbException {
            for ( Page p: pagelist ) {
                p.markDirty(true,tid);
                if ( bufferpool.size() > numPages ) {
                   //use the evict policy
                   evictPage();
                }
                //获取节点，此时的页一定已经在缓存了，因为刚刚被修改的时候已经放入缓存了
                LinkedNode node = bufferpool.get(p.getId());
                node.page = p;
                //update to bufferpool
                bufferpool.put(p.getId(), node);
            }
    }
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (PageId pid : bufferpool.keySet() ) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        bufferpool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page page = bufferpool.get(pid).page;
        if (page.isDirty() != null ) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        LinkedNode tail = removeTail();
        PageId evictPageId = tail.pageId;
        try {
            flushPage(evictPageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        discardPage(evictPageId);
    }

}
