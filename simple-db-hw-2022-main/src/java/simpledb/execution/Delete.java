package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private int count;
    private boolean open;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here
        this.t=t;
        this.child=child;
        this.td=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"delete nums"});
        this.count=0;
        this.open=false;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.count=0;
        child.open();
        super.open();
    }

    public void close() {
        // TODO: some code goes here
        this.count=0;
        this.child=null;
        super.close();
        open=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.count=0;
        child.rewind();
        open=false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (open) {
            return null;
        }
        this.open = true;
        while (this.child.hasNext()) {
            Tuple p=child.next();
            try {
                Database.getBufferPool().deleteTuple(t,p);
                this.count++;
            } catch (IOException e) {
                throw new DbException("insertTuple failed");
            }
        }
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(this.count));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        child=children[0];
    }

}
