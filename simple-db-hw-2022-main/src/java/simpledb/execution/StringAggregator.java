package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private  int afield;
    private Op what;
    private Map<Field,Integer> resultMap;
    private TupleDesc aggDesc;
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        if(!what.equals(Op.COUNT))
        {
            throw new IllegalArgumentException("NOT SUPPORT!");
        }
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.resultMap=new HashMap<>();
        if (this.gbfield>=0) {
            // ÓÐgroupBy
            this.aggDesc = new TupleDesc(new Type[]{this.gbfieldtype,Type.INT_TYPE},
                    new String[]{"groupVal","aggregateVal"});
        } else {
            // ÎÞgroupBy
            this.aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        StringField afield=(StringField) tup.getField(this.afield);
        Field gbfield=this.gbfield==NO_GROUPING?null: tup.getField(this.gbfield);
        String value=afield.getValue();
        if(gbfield!=null&&gbfield.getType()!=this.gbfieldtype)
        {
            throw new IllegalArgumentException();
        }
        if(this.resultMap.containsKey(gbfield))
        {
            this.resultMap.put(gbfield,this.resultMap.get(gbfield)+1);
        }
        else
        {
            this.resultMap.put(gbfield,1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return new StringIterator();
    }

    private class StringIterator implements OpIterator {
        private Iterator<Map.Entry<Field,Integer>> it;
        @Override
        public void open() throws DbException, TransactionAbortedException {
            it=resultMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tp=new Tuple(aggDesc);
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
            this.it=null;
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
