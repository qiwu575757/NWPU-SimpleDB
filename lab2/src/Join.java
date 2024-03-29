package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate jp;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t1;
    private Tuple t2;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.jp = p;
        this.child1 = child1;
        this.child2 = child2;
        t1 = null;
        t2 = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(jp.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(jp.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.child1.open();
        this.child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        t1 = null;
        t2 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        TupleDesc td1 = child1.getTupleDesc(), td2 = child2.getTupleDesc();
        while ( child1.hasNext() || t1 != null ) {
            if ( child1.hasNext() && t1 == null ) {
                t1 = child1.next();
            }
            while ( child2.hasNext() ) {
                t2 = child2.next();
                if ( jp.filter(t1, t2) ) {
                    //merge
                    TupleDesc td = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
                    Tuple result = new Tuple(td);
                    int i = 0;
                    for ( ; i < td1.numFields(); i++ )
                    {
                        result.setField(i, t1.getField(i));
                    }
                    for ( int j = 0; j < td2.numFields(); j++ )
                    {
                        result.setField(i + j, t2.getField(j));
                    }
                    // t1 的意义就是 笛卡尔积, 一个可能对多个相等
                    //如果child2指针恰好处于元组最后一个位置，需要重置
                    if ( !child2.hasNext() ) {
                        child2.rewind();
                        t1 = null;
                    }

                    return result;
                }
            }
            //child2迭代完成后需要重置
            child2.rewind();
            t1 = null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {
            child1,
            child2
        };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }

}
