package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

class AggregateIterator implements OpIterator {

    protected Iterator<Map.Entry<Field, Integer>> it;
    TupleDesc td;

    private Map<Field, Integer> groupMap;
    protected Type itgbfieldtype;

    public AggregateIterator(Map<Field, Integer> groupMap, Type gbfieldtype) {
        this.groupMap = groupMap;
        this.itgbfieldtype = gbfieldtype;
        // no grouping
        if (this.itgbfieldtype == null)
            this.td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateVal"});
        else
            this.td = new TupleDesc(new Type[] {this.itgbfieldtype, Type.INT_TYPE}, new String[] {"groupVal", "aggregateVal"});
    }


    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = this.it.next();
        Field f = entry.getKey();
        Tuple rtn = new Tuple(this.td);
        this.setFields(rtn, entry.getValue(), f);
        return rtn;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void close() {
        this.it = null;
        this.td = null;
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