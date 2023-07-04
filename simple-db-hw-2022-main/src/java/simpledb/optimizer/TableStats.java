package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * TableStats 表示有关基表中的统计信息（例如，直方图）查询。
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;//每页IO的开销

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
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
     * 直方图的条柱数。您可以随意将此值增加到 100 以上，尽管我们的测试假设直方图中至少有 100 个条柱数。
     */
    static final int NUM_HIST_BINS = 100;

    private int numtuples;//元组的数量
    private int numpages;//页面数量
    private int numfield;//字段的数量
    private int iocostperpage;//每页io的代价
    private int tableid;//用于计算统计信息的表
    private Map<Integer,IntHistogram> intHistogramMap;
    private Map<Integer,StringHistogram> stringHistogramMap;
    private Map<Integer,Integer> max;//最大字段映射
    private Map<Integer,Integer> min;//最小字段映射
    private DbFile dbFile;
    private HeapFile table;//需要进行数据统计的表
    private TupleDesc td;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 创建一个新的 TableStats 对象，用于跟踪表的每一列的统计信息
     *
     * @param tableid       The table over which to compute statistics用于计算统计信息的表
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.每页的代价，这不会区分顺序扫描 IO 和磁盘寻道
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here
        this.tableid=tableid;
        this.iocostperpage=ioCostPerPage;
        this.intHistogramMap=new HashMap<>();
        this.stringHistogramMap=new HashMap<>();
        this.dbFile=Database.getCatalog().getDatabaseFile(tableid);
        this.table=(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.numtuples=0;
        this.numpages=table.numPages();
        this.numfield=table.getTupleDesc().numFields();
        this.max=new HashMap<>();
        this.min=new HashMap<>();
        this.td=dbFile.getTupleDesc();
        Transaction t=new Transaction();
        t.start();
        DbFileIterator iterator=table.iterator(t.getId());
        try{
            iterator.open();
            while (iterator.hasNext())
            {
                Tuple tp=iterator.next();
                this.numtuples++;
                for(int i=0;i<td.numFields();i++)
                {
                    //计算int
                    if(td.getFieldType(i).equals(Type.INT_TYPE))
                    {
                        IntField intField= (IntField) tp.getField(i);
                        int value=intField.getValue();
                        //计算int的min
                        if(min.get(i)==null||value<min.get(i))
                        {
                            min.put(i,value);
                        } else if (max.get(i)==null||value>max.get(i)) {
                            max.put(i,value);
                        }
                    }
                    else if(td.getFieldType(i).equals(Type.STRING_TYPE)){
                        StringField stringField=(StringField) tp.getField(i);
                        StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                        stringHistogram.addValue(stringField.getValue());
                        stringHistogramMap.put(i,stringHistogram);

                    }
                }
            }
            //根据最大最小值构造直方图
            for (int i = 0; i < td.numFields(); i++) {
                if(min.get(i)!=null)
                {
                    this.intHistogramMap.put(i,new IntHistogram(NUM_HIST_BINS,min.get(i),max.get(i)));
                }

            }
            iterator.rewind();
            //添加值
            while (iterator.hasNext())
            {
                Tuple tp=iterator.next();
                for(int i=0;i< td.numFields();i++)
                {
                    if(td.getFieldType(i).equals(Type.INT_TYPE))
                    {
                        IntField f=(IntField)tp.getField(i);
                        IntHistogram in=intHistogramMap.get(i);
                        in.addValue(f.getValue());
                        this.intHistogramMap.put(i,in);
                    }
                }
            }


        }  catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e);
        }finally {
            iterator.close();
            try {
                t.commit();
            }catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO: some code goes here
        return table.numPages()*iocostperpage*2;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table任何谓词对表的选择性
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return (int)(numtuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     *              谓词中的运算符该方法的语义是，给定表，然后给定一个元组，我们不知道字段的值，
     *              返回预期的选择性。您可以从直方图中估计此值。
     */
    //判断类型分别调用int和string的直方图
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        if(td.getFieldType(field).equals(Type.INT_TYPE))
        {
            return intHistogramMap.get(field).avgSelectivity();
        }
        else if (td.getFieldType(field).equals(Type.STRING_TYPE))
        {
            return stringHistogramMap.get(field).avgSelectivity();
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges谓词范围的字段
     * @param op       The logical operation in the predicate谓词中的逻辑运算
     * @param constant The value against which the field is compared 与字段进行比较的值
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate估计的选择性（满足谓词的元组分数）
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // TODO: some code goes here
        if (td.getFieldType(field).equals(Type.INT_TYPE)) {
            IntField intField = (IntField) constant;
            return intHistogramMap.get(field).estimateSelectivity(op,intField.getValue());
        } else if(td.getFieldType(field).equals(Type.STRING_TYPE)){
            StringField stringField = (StringField) constant;
            return stringHistogramMap.get(field).estimateSelectivity(op,stringField.getValue());
        }
        return -1.00;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // TODO: some code goes here
        return numtuples;
    }

}
