package simpledb.execution;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfieldIndex;
    private Type gbfieldtype;
    private int afieldIndex;
    private Op what;
    //自定义接口
    private AggHandler aggHandler;

    private abstract class AggHandler {
        HashMap<Field,Integer> aggResult;

        //gbField = 分组的field, aggField = 聚合运算的结果
        abstract void handle(Field gbField, IntField aggField);

        public AggHandler() {
            aggResult = new HashMap<>();
        }
        public HashMap<Field,Integer> getAggResult() {
            return aggResult;
        }
    }

    public class CountHandler extends AggHandler {
        @Override
        void handle(Field gbField, IntField aggField) {
            if (aggResult.containsKey(gbField)) {
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            }
            else {
                aggResult.put(gbField, 1);
            }
        }
    }

    private class SumHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + value);
            } else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class MaxHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, Math.max(aggResult.get(gbField) , value));
            } else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class MinHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, Math.min(aggResult.get(gbField) , value));
            } else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class AvgHandler extends  AggHandler{
        HashMap<Field, Integer> sum;
        HashMap<Field, Integer> count;
        private AvgHandler(){
            sum = new HashMap<>();
            count = new HashMap<>();
        }
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();

            if(sum.containsKey(gbField) && count.containsKey(gbField)){
                sum.put(gbField, sum.get(gbField) + value);
                count.put(gbField, count.get(gbField) + 1);
            } else {
                sum.put(gbField, value);
                count.put(gbField, 1);
            }
            int avg = sum.get(gbField) / count.get(gbField);
            aggResult.put(gbField, avg);
        }
    }
    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldIndex = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afieldIndex = afield;
        this.what = what;

        switch ( what ){
            case MIN:
                aggHandler = new MinHandler();
                break;
            case MAX:
                aggHandler = new MaxHandler();
                break;
            case SUM:
                aggHandler = new SumHandler();
                break;
            case AVG:
                aggHandler = new AvgHandler();
                break;
            case COUNT:
                aggHandler = new CountHandler();
                break;
            default:
                throw new UnsupportedOperationException("InterAggregator Error: Unsupported Operator.");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */

    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        IntField aggField = (IntField) tup.getField(afieldIndex);
        //对于有分组的聚合运算，以(gbField, aggField)的形式，保存原tuple进行分组聚合后每个分组对应的聚合结果
        //对于没有分组的聚合运算，td只有一个字段来保存聚合结果
        if(gbfieldIndex == NO_GROUPING){
            gbField = null;
        } else {
            gbField = tup.getField(gbfieldIndex);
        }
        aggHandler.handle(gbField,aggField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //获取结果聚合集
        HashMap<Field,Integer> aggResult = aggHandler.getAggResult();
        //构建 tuple
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        //存储结果 tuple
        ArrayList<Tuple> tuples = new ArrayList<>();

        //if have no group
        if ( gbfieldIndex == NO_GROUPING )
        {
            types = new Type[] {
                Type.INT_TYPE
            };
            names = new String[] {
                "aggregateValue"
            };
            tupleDesc = new TupleDesc(types,names);
            //生成结果，设置字段值
            Tuple tuple = new Tuple(tupleDesc);
            IntField resultField = new IntField(aggResult.get(null));
            tuple.setField(0, resultField);
            tuples.add(tuple);
        }
        else
        {
            types = new Type[] {
                gbfieldtype,
                Type.INT_TYPE
            };
            names = new String[] {
                "groupValue",
                "aggregateValue"
            };
            tupleDesc = new TupleDesc(types,names);
            for ( Field field: aggResult.keySet() ) {
                Tuple tuple = new Tuple(tupleDesc);
                if (gbfieldtype == Type.INT_TYPE) {
                    IntField intField = (IntField)field;
                    tuple.setField(0, intField);
                }
                else
                {
                    StringField stringField = (StringField)field;
                    tuple.setField(0, stringField);
                }

                IntField resultField = new IntField(aggResult.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        }

        return new TupleIterator(tupleDesc, tuples);
    }

}
