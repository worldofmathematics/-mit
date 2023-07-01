package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private final TupleDesc td;

    // helper for fetchNext
    private int counter;
    private boolean open;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("TupleDesc dose not match");
        }
        this.t= t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"insert nums"});
        this.counter = 0;
        this.open = false;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.counter = 0;
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        this.child.close();
        this.counter = 0;
        this.open = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.counter = 0;
        this.child.rewind();
        this.open = false;
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
     *         null if called more than once.//如果访问多从作为null
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (open) {
            return null;
        }
        this.open = true;
        while (this.child.hasNext()) {
            Tuple p=child.next();
            try {
                Database.getBufferPool().insertTuple(t,tableId,p);
                this.counter++;
            } catch (IOException e) {
                throw new DbException("insertTuple failed");
            }
        }
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(this.counter));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this. child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
