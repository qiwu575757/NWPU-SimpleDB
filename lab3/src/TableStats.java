package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.swing.plaf.TreeUI;
import javax.swing.plaf.basic.BasicBorders.FieldBorder;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 *
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    //需要进行数据统计的表
    private HeapFile table;
    //读取每页的IO成本
    private int ioCostPerPage;
    //表中tuple的总数
    private int tuplesNum;
    //表中page的总数,用于计算扫描成本
    private int pagesNum;
    //整型字段与其直方图的映射
    private HashMap<Integer,IntHistogram> integerIntHistogramMap;
    //字符串型字段与其直方图的映射
    private HashMap<Integer,StringHistogram> stringIntHistogramMap;
    //字段与该字段中最大值的映射
    private HashMap<Integer,Integer> maxField;
    //字段与该字段中最小值的映射
    private HashMap<Integer,Integer> minField;
    //表中的所有tuples
    private ArrayList<Tuple> tuples;
    //表的属性行
    private TupleDesc tupleDesc;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        table = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
        tupleDesc = table.getTupleDesc();
        pagesNum = table.numPages();
        DbFileIterator iterator = table.iterator(new TransactionId());
        int fieldNum = table.getTupleDesc().numFields();

        maxField = new HashMap<>();
        minField = new HashMap<>();
        integerIntHistogramMap = new HashMap<>();
        stringIntHistogramMap = new HashMap<>();
        this.ioCostPerPage = ioCostPerPage;
        tuples = new ArrayList<>();
        tuplesNum = 0;

        try {
            iterator.open();
            //get the maxValue and minValue of every field
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                tuples.add(tuple);
                tuplesNum++;
                for ( int i = 0; i < fieldNum; i++ ) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        int value = ((IntField)tuple.getField(i)).getValue();
                        if ( maxField.get(i) == null || maxField.get(i) < value)
                            maxField.put(i, value);
                        if ( minField.get(i) == null || minField.get(i) > value)
                            minField.put(i, value);
                    }
                }
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //iterate each field to get the Histogram
        for ( int i = 0; i < fieldNum; i++ )
        {
            Iterator<Tuple> tupleIterator = tuples.iterator();
            Type fieldType = tupleDesc.getFieldType(i);
            if (fieldType.equals(Type.INT_TYPE)) {
                int minValue = minField.get(i);
                int maxValue = maxField.get(i);
                IntHistogram histogram = new IntHistogram(NUM_HIST_BINS, minValue, maxValue);
                try {
                    while (tupleIterator.hasNext()) {
                        Tuple tuple = tupleIterator.next();
                        int fieldValue = ((IntField)tuple.getField(i)).getValue();
                        histogram.addValue(fieldValue);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                integerIntHistogramMap.put(i, histogram);
            }
            else {
                StringHistogram histogram = new StringHistogram(NUM_HIST_BINS);
                try {
                    while (tupleIterator.hasNext()) {
                        Tuple tuple = tupleIterator.next();
                        String fieldValue = ((StringField)tuple.getField(i)).getValue();
                        histogram.addValue(fieldValue);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                stringIntHistogramMap.put(i, histogram);
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return pagesNum * ioCostPerPage * 1.0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // 选择率（selectivityFactor=基数/行数(tuples)
        return (int)(tuplesNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        Type fieldType = tupleDesc.getFieldType(field);
        if ( fieldType.equals(Type.INT_TYPE)) {
            IntHistogram histogram = integerIntHistogramMap.get(field);
            return histogram.avgSelectivity();
        }
        else {
            StringHistogram histogram = stringIntHistogramMap.get(field);
            return histogram.avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = tupleDesc.getFieldType(field);
        if ( fieldType.equals(Type.INT_TYPE)) {
            IntHistogram histogram = integerIntHistogramMap.get(field);
            int fieldValue = ((IntField)constant).getValue();
            return histogram.estimateSelectivity(op,fieldValue);
        }
        else {
            StringHistogram histogram = stringIntHistogramMap.get(field);
            String fieldValue = ((StringField)constant).getValue();
            return histogram.estimateSelectivity(op,fieldValue);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tuplesNum;
    }

}
