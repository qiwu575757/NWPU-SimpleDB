package simpledb.execution;

import simpledb.common.Type;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    //the inserted tuple iterator
    private OpIterator child;
    //the inserted table
    private int tableId;
    //returning a single tuple with one integer field ,containing the count
    private TupleDesc tupleDesc;
    //the flag for insert
    private boolean inserted;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if ( !child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc() )) {
            throw new DbException("Error:insert type error");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(
            new Type[] { Type.INT_TYPE },
            new String[] { " the number of inserted tuples"}
            );
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @throws IOException
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if ( !inserted ) {
            inserted = true;
            int count = 0;
            while ( child.hasNext() ) {
                Tuple tuple = child.next();
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                    count++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Tuple tuple = new Tuple(this.tupleDesc);
            tuple.setField(0, new IntField(count));
            return tuple;
        }
        
        return null;
    }


    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {
            child
        };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
