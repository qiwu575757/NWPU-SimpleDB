package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.HeapPageId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
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
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        final int pagesize = Database.getBufferPool().getPageSize();
        byte[] databuffer = HeapPage.createEmptyPageData();

        try {
            FileInputStream input = new FileInputStream(this.f);
            input.skip(pgNo * pagesize);
            input.read(databuffer);

            return new HeapPage((HeapPageId)pid, databuffer);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        } catch (IOException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        //get page num
        int pgNo = page.getId().getPageNumber();
        if ( pgNo > numPages() ) {
            throw new IllegalArgumentException();
        }
        //create writing tool
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        // fileptr = pageNo * pagesize
        file.seek(pgNo * BufferPool.getPageSize());
        //write data
        file.write(page.getPageData());

        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int filebytes = (int)this.f.length();
        int pagesize = Database.getBufferPool().getPageSize();
        return filebytes/pagesize;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        //select page
        for ( int pageNo = 0; pageNo < numPages(); pageNo++ )
        {
            HeapPageId pid = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            //find this page have empty space or not
            if ( page.getNumEmptySlots() != 0 ) {
                page.insertTuple(t);
                list.add(page);
                return list;
            }
            else {
                // 完善：当该tuple上没有空slot时，释放该page上的锁（之后又申请了锁，违反了两段锁协议），方便其他事务访问
                Database.getBufferPool().unsafeReleasePage(tid, pid);
                continue;
            }
        }

        //if all pages are full, should create new page to insert
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(f, true));
        //create empty page in the file
        byte[] emptypage = HeapPage.createEmptyPageData();
        output.write(emptypage);
        //close() would  flush data to disk
        output.close();

        //create new page and insert tuple to it
        HeapPageId pid = new HeapPageId(getId(), numPages()-1);
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        //find the page
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        //An ArrayList contain the pages that were modified
        list.add(page);
        return list;
    }

    public class HeapFileIterator implements DbFileIterator
    {
        private Integer currentPage;
        private Iterator<Tuple> iter;
        private TransactionId transId;
        private Integer numpages;
        private Integer tableid;

        public HeapFileIterator(TransactionId tid)
        {
            this.currentPage = null;
            this.iter = null;
            this.transId = tid;
            this.numpages = numPages();
            this.tableid = getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException
        {
            this.currentPage = 0;//used for rwind,otherise currentPage can be set ahead
            this.iter = getIterator(this.currentPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException
        {
            //for test: not open
            if ( this.currentPage != null )
            {
                if ( this.currentPage < this.numpages )
                {
                    if ( this.iter.hasNext() )
                    {
                        return true;
                    }
                    else
                    {
                        this.currentPage += 1;
                        if (this.currentPage == this.numpages)
                            return false;
                        else//if current == numpages, getIterator will error
                            this.iter = getIterator(this.currentPage);
                    }
                    return hasNext();
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        @Override
        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException
        {
            if ( hasNext() )
            {
                return this.iter.next();
            }
            throw new NoSuchElementException("HeapFile: run out");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException
        {
            close();
            open();
        }

        @Override
        public void close()
        {
            this.currentPage = null;
            this.iter = null;
        }

        public Iterator<Tuple> getIterator(int current)
                        throws TransactionAbortedException, DbException
        {
            PageId pid = new HeapPageId(this.tableid,current);
            Permissions perm = Permissions.READ_ONLY;
            return ((HeapPage)Database.getBufferPool()
                        .getPage(transId, pid, perm)).iterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}
