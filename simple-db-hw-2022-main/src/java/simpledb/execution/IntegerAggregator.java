package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * 聚合
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private TupleDesc aggDesc;
    private final Map<Field,Integer> resultgroup;//MIN, MAX, SUM,COUNT
    private final Map<Field, List<Integer>> avggroup;//AVG

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     *                    （元组中分组依据字段的从 0 开始的索引，如果没有分组，则为 NO_GROUPING）
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     *                    （分组依据字段的类型（例如，Type.INT_TYPE），如果没有分组，则为 null）
     * @param afield      the 0-based index of the aggregate field in the tuple
     *                    （元组中聚合字段的从 0 开始的索引）
     * @param what        the aggregation operator（聚合运算符）
     *                    MIN, MAX, SUM, AVG, COUNT,(SUM_COUNT,SC_AVG)lab7;
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.resultgroup=new HashMap<>();
        this.avggroup=new HashMap<>();
        if (this.gbfield>=0) {
            // 有groupBy
            this.aggDesc = new TupleDesc(new Type[]{this.gbfieldtype,Type.INT_TYPE},
                    new String[]{"groupVal","aggregateVal"});
        } else {
            // 无groupBy
            this.aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     *            （包含聚合字段和分组依据字段的元组）
     */
    /**
     * select MAX(Sage) FROM Student Where Sdept='CS';
     * select MIN(Sage) FROM Student where Sdept='CS';
     * select sum(money) as 总收入 from table GROUP BY xx
     * select COUNT(Grade) FROM SC;
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        //TODO:(SUM_COUNT,SC_AVG)lab7;
        // TODO: some code goes here
        IntField afield=(IntField) tup.getField(this.afield);//获取聚合字段
        Field gbfield=this.gbfield==NO_GROUPING?null: tup.getField(this.gbfield);
        int value=afield.getValue();
        if(gbfield!=null&&gbfield.getType()!=this.gbfieldtype)
        {
            throw new IllegalArgumentException();
        }
        switch (this.what){
            case MAX://判断gbfield是否值，如果有进行比较放入较大的，如果没有直接放入
                if(this.resultgroup.containsKey(gbfield))
                {
                    this.resultgroup.put(gbfield,Math.max(this.resultgroup.get(gbfield),value));
                }
                else
                {
                    this.resultgroup.put(gbfield,value);
                }
                break;
            case MIN://判断gbfield是否有值，如果有放入较小的，如果没有直接放入
                if(this.resultgroup.containsKey(gbfield))
                {
                    this.resultgroup.put(gbfield,Math.min(this.resultgroup.get(gbfield),value));
                }
                else
                {
                    this.resultgroup.put(gbfield,value);
                }
                break;
            case SUM://判断gbfield是否有值，如果有放入相加放入，如果没有直接放入
                if(this.resultgroup.containsKey(gbfield))
                {
                    this.resultgroup.put(gbfield,this.resultgroup.get(gbfield)+value);
                }
                else
                {
                    this.resultgroup.put(gbfield,value);
                }
                break;
            case COUNT://判断gbfield是否有值，如果有计数加1，如果没有直接放入
                if(!this.resultgroup.containsKey(gbfield))
                {
                    this.resultgroup.put(gbfield,1);
                }
                else
                {
                    this.resultgroup.put(gbfield,this.resultgroup.get(gbfield)+1);
                }
                break;
            case AVG://利用数组存放平均值
                if(!this.avggroup.containsKey(gbfield))//gbfield不含有值，直接放入即为平均值
                {
                    List<Integer> l=new ArrayList<>();
                    l.add(value);
                    this.avggroup.put(gbfield,l);
                }
                else//如果有则更新和还有长度计算平均值
                {
                    List<Integer> l = this.avggroup.get(gbfield);
                    l.add(value);
                    int sum=0;
                    for (Integer integer : l) {
                        sum += integer;
                    }
                    List<Integer> l1=new ArrayList<>();
                    l1.add(sum /l.size());
                    this.avggroup.put(gbfield,l1);
                }
                break;
            default:
                throw new IllegalArgumentException("Aggregate not supported!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {//返回迭代器
        // TODO: some code goes here
        return new IntAggIterator();
    }

    private class IntAggIterator implements OpIterator {
        //利用Map.Entry返回一对键值
        private Iterator<Map.Entry<Field, List<Integer>>> avgIt;//保存AVG
        private  Iterator<Map.Entry<Field,Integer>> it;
        /*
        在每个函数里判断是否为AVG
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            if(what.equals(Op.AVG))
            {
                avgIt=avggroup.entrySet().iterator();
            }
            it=resultgroup.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(what.equals(Op.AVG))
            {
                return avgIt.hasNext();
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tp=new Tuple(aggDesc);
            if(what.equals(Op.AVG))
            {
                Map.Entry<Field, List<Integer>> avgOrSumCountEntry = this.avgIt.next();
                Field avgOrSumCountField = avgOrSumCountEntry.getKey();
                List<Integer> avgOrSumCountList = avgOrSumCountEntry.getValue();
                int value = this.sumList(avgOrSumCountList) / avgOrSumCountList.size();
                this.setFields(tp, value, avgOrSumCountField);
                return tp;
            }
            Map.Entry<Field,Integer> m=this.it.next();
            Field mfield=m.getKey();
            this.setFields(tp,m.getValue(),mfield);
            return tp;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggDesc;
        }

        @Override
        public void close() {
        }
        private int sumList(List<Integer> l) {
            int sum = 0;
            for (int i : l)
                sum += i;
            return sum;
        }
        void setFields(Tuple rtn, int value, Field f) {
            if (f == null) {
                rtn.setField(0, new IntField(value));
            } else {
                rtn.setField(0, f);
                rtn.setField(1, new IntField(value));
            }
        }
    }
}

