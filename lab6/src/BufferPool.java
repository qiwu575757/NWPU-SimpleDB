package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class PageLock{
    //Lock Type
    public static final int SHARE = 0;
    public static final int EXCLUSIVE = 1;
    private TransactionId tid;
    private int type;

    public PageLock(TransactionId tid, int type) {
        this.tid = tid;
        this.type = type;
    }

    public TransactionId getTid() {
        return this.tid;
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }
}

class LockManager {
    ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, int requireType)
            throws TransactionAbortedException, InterruptedException {
        //current page have no locks,get lock directly
        if (lockMap.get(pageId) == null) {
            PageLock pageLock = new PageLock(tid, requireType);
            ConcurrentHashMap<TransactionId,PageLock> tid_pagelocks = new ConcurrentHashMap<>();
            tid_pagelocks.put(tid,pageLock);
            this.lockMap.put(pageId,tid_pagelocks);

            return true;
        }
        //current page have locks
        else {
            ConcurrentHashMap<TransactionId,PageLock> tid_pagelocks = this.lockMap.get(pageId);
            // 当前页面没有tid事务的锁
            if (tid_pagelocks.get(tid) == null) {
                // 当前页面有多个其他事务的锁（读锁）
                if (tid_pagelocks.size() > 1) {
                    if (requireType == PageLock.SHARE) {
                        PageLock pageLock = new PageLock(tid, requireType);
                        tid_pagelocks.put((tid), pageLock);
                        lockMap.put(pageId, tid_pagelocks);

                        return true;
                    }
                    else{
                        wait(20);

                        return false;
                    }
                }
                // 当前页面有一个其他事务的锁（读/写锁）
                else if (tid_pagelocks.size() == 1) {
                    // 这个锁不是当前事务的锁，不能直接get(tid)
                    // PageLock curLock = tid_pagelocks.get(tid);
                    PageLock curLock = null;
                    for (PageLock lock : tid_pagelocks.values()){
                        curLock = lock;
                    }
                    // current lock is read lock
                    if (curLock.getType() == PageLock.SHARE) {
                        if (requireType == PageLock.SHARE) {
                            PageLock pageLock = new PageLock(tid, requireType);
                            tid_pagelocks.put((tid), pageLock);
                            lockMap.put(pageId, tid_pagelocks);

                            return true;
                        }
                        else {// write lock
                            wait(10);

                            return false;
                        }
                    }
                    // current lock is write lock
                    else {
                        wait(10);

                        return false;
                    }
                }
            }
            // current page have tid's lock
            else { // (tid_pagelocks.get(tid) != null)
                PageLock pageLock = tid_pagelocks.get(tid);
                // tid has page's read lock
                if (pageLock.getType() == PageLock.SHARE) {
                    // tid need require read lock
                    if (requireType == PageLock.SHARE) {
                        return true; // return directly
                    }
                    // tid need require write lock
                    else {
                        // The page only have the tid's read lock
                        if (tid_pagelocks.size() == 1) {
                            pageLock.setType(PageLock.EXCLUSIVE);
                            tid_pagelocks.put(tid, pageLock);

                            return true;
                        }
                        else {
                            // The page have other tid's lock,couldn't update
                            throw new TransactionAbortedException();
                        }
                    }
                }
                // tid has page's write lock
                else {
                    return true;
                }
            }
        }

        return false;
    }

    // Tid have lock of certain page's lock or not
    public synchronized boolean isholdlock(TransactionId tid, PageId pid) {
        ConcurrentHashMap<TransactionId,PageLock> tid_pagelocks;
        tid_pagelocks = lockMap.get(pid);
        if (tid_pagelocks == null) {
            return false;
        }
        else {
            PageLock pageLock = tid_pagelocks.get(tid);
            if (pageLock == null) {
                return false;
            }
            else {
                return true;
            }
        }
    }

    // release tid's locks in certain page
    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if (isholdlock(tid, pid)) {
            ConcurrentHashMap<TransactionId,PageLock> tid_pagelocks;
            tid_pagelocks = lockMap.get(pid);
            tid_pagelocks.remove(tid);

            // This page only have the tid's locks
            if (tid_pagelocks.size() == 0) {
                lockMap.remove(pid);
            }
            this.notify();

            return true;
        }

        return false;
    }

    // Release all tid's lock when the tid is over
    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> pageIds = lockMap.keySet();
        for (PageId pageId: pageIds) {
            releaseLock(tid, pageId);
        }
    }
}
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
    private LockManager lockManager;
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

        public void setPage(Page page) {
            this.page = page;
            this.pageId = page.getId();
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
        lockManager = new LockManager();
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int locktype = (perm == Permissions.READ_ONLY) ? PageLock.SHARE : PageLock.EXCLUSIVE;
        long st = System.currentTimeMillis();
        while (true) {
            // acquire lock, othersize will block
            try {
                if (lockManager.acquireLock(pid,tid,locktype)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            /*
            deadlock judgement:
                超时。对每个事务设置一个获取锁的超时时间，如果在超时时间内获取不到锁，
                我们就认为可能发生了死锁，将该事务进行中断。
            */
            long now = System.currentTimeMillis();
            if ( now - st > 500 )
                throw new TransactionAbortedException();
        }

        // -------------------Lab3 do
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
     *当事务向page中的空slot插入tuple时，首先需要获取该page上的锁。如果该page上没有空的slot，需要访问下一个page，
     如果该HeapFile上所有的page都已满，需要创建一个新的page将tuple插入其中。但是，虽然该事务不会对已满的page进行操作，
     可此时它依旧持有这些page上的锁，其它事务也不能访问这些page，所以当判断某一page上的slot已满时，
     需要释放掉该page上的锁。虽然这不满足两段锁协议（释放掉锁后又再次申请），但该事务并没有使用page中的任何数据，后序也不会用到，
     所以并不会有任何影响，而且也可以让其他事务访问那些slot已满的page。

     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
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
        if (commit) {
            try {
                // If commit, write all pages to disk
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            // If fail to commit, read the dity page from disk to recover
            restorePages(tid);
        }
        // Release all lock about this tid after dirty pages are handled
        lockManager.completeTransaction(tid);
    }

    public synchronized void restorePages(TransactionId tid) {
        for (LinkedNode node: bufferpool.values()) {
            Page page = node.page;
            PageId pid = node.pageId;
            if (tid.equals(page.isDirty())) {
                // read the page related to dirty page from the disk
                int tableId = pid.getTableId();
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                Page pageFromDisk = dbFile.readPage(pid);
                // restore the dirty page
                node.setPage(pageFromDisk);
                bufferpool.put(pid,node);
                moveToHead(node);
            }
        }
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
        // not necessary for lab1
        DbFile heapFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> arrayList =  heapFile.insertTuple(tid,t);
        for(Page page : arrayList){
            PageId pid = page.getId();
            page.markDirty(true,tid);

            if(!bufferpool.containsKey(pid)){
                LinkedNode node = new LinkedNode(pid,page);
                if(bufferpool.size() < numPages){
                    addToHead(node);
                    bufferpool.put(pid,node);
                } else {
                    evictPage();
                    addToHead(node);
                    bufferpool.put(pid,node);
                }
            } else {
                LinkedNode node = bufferpool.get(pid);
                moveToHead(node);
                node.page = page;
                bufferpool.put(pid,node);
            }
        }
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile heapFile =  Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> arrayList = heapFile.deleteTuple(tid,t);
        for(Page page : arrayList){
            page.markDirty(true,tid);
            LinkedNode node = bufferpool.get(page.getId());
            if(node == null){
                if(bufferpool.size() < numPages){
                    LinkedNode temp = new LinkedNode(page.getId(),page);
                    addToHead(temp);
                    bufferpool.put(page.getId(),temp);
                } else {
                    evictPage();
                    LinkedNode temp = new LinkedNode(page.getId(),page);
                    addToHead(temp);
                    bufferpool.put(page.getId(),temp);
                }
            } else {
                LinkedNode temp = bufferpool.get(page.getId());
                moveToHead(temp);
                bufferpool.put(page.getId(),temp);
            }
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
        if (bufferpool.containsKey(pid) ) {
            Page page = bufferpool.get(pid).page;
            // append an update record to the log, with
            // a before-image and after-image.
            //get the page
            TransactionId dirtier = page.isDirty();
            if (dirtier != null){
              Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
              Database.getLogFile().force();
            }
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid: lockManager.lockMap.keySet()) {
            // ConcurrentHashMap<TransactionId,PageLock> tid_lockmap = lockManager.lockMap.get(pid);
            Page p = bufferpool.get(pid).page;
            p.setBeforeImage();
            if (p.isDirty() == tid) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 当需要置换的page是dirty page时，需要跳过此page，去置换下一个非dirty的page。
     * 当BufferPool中缓存的page都是dirty page时，抛出异常。
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        for (int i = 0; i < numPages; i++) {
            LinkedNode tail = removeTail();
            Page evictPage = tail.page;
            if (evictPage.isDirty() != null) {
                addToHead(tail);
            }
            else {
                PageId pid = tail.pageId;
                discardPage(pid);
                return;
            }
        }
        throw new DbException("Erorr: all pages in bufferpool are dirty");
    }

}
