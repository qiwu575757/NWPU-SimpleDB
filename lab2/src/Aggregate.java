package simpledb.execution;

import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    // 进行聚合操作的类
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator opIterator;
    // 聚合结果的属性行
    private TupleDesc tupleDesc;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gfieldType = (gfield == Aggregator.NO_GROUPING ) ? null
                    : child.getTupleDesc().getFieldType(gfield);
        Type afieldType = child.getTupleDesc().getFieldType(afield);
        //create aggregator
        if ( afieldType == Type.STRING_TYPE ) {
            this.aggregator = new StringAggregator(gfield, gfieldType, afield, aop);
        }
        else {
            this.aggregator = new IntegerAggregator(gfield, gfieldType, afield, aop);
        }

        //组建 TupleDesc
        List<Type> types = new ArrayList<>();
        List<String> fieldnames = new ArrayList<>();
        if ( gfieldType != null ) {
            types.add(gfieldType);
            fieldnames.add(child.getTupleDesc().getFieldName(gfield));
        }
        types.add(child.getTupleDesc().getFieldType(afield));
        fieldnames.add(child.getTupleDesc().getFieldName(afield));

        //here is something unknown
        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            types.add(Type.INT_TYPE);
            fieldnames.add("COUNT");
        }
        this.tupleDesc = new TupleDesc(types.toArray(new Type[types.size()]),
                                fieldnames.toArray(new String[fieldnames.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        // if (gfield == Aggregator.NO_GROUPING)
        //     return child.getTupleDesc().getFieldName(i);
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,TransactionAbortedException {
        // some code goes here
        // 聚合所有的tuple
        child.open();
        while(child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        // 获取聚合后的迭代器
        opIterator = aggregator.iterator();
        // 查询
        opIterator.open();
        // 使父类状态保持一致
        super.open();
    }


    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if ( opIterator.hasNext() ) {
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();;
        opIterator.rewind();;
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        opIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {
            child
        };
    }
    // 设置初始字段，相当于二次设置，因此需要更新类型组
    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
        Type gfieldType = child.getTupleDesc().getFieldType(gfield);
        
        //组建 TupleDesc
        List<Type> types = new ArrayList<>();
        List<String> fieldnames = new ArrayList<>();
        if ( gfieldType != null ) {
            types.add(gfieldType);
            fieldnames.add(child.getTupleDesc().getFieldName(gfield));
        }
        types.add(child.getTupleDesc().getFieldType(afield));
        fieldnames.add(child.getTupleDesc().getFieldName(afield));

        //here is something unknown
        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            types.add(Type.INT_TYPE);
            fieldnames.add("COUNT");
        }
        this.tupleDesc = new TupleDesc(types.toArray(new Type[types.size()]),
                                fieldnames.toArray(new String[fieldnames.size()]));
    }

}
