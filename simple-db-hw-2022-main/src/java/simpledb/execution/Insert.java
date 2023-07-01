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

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private DbFile dbFile;
    private final TupleDesc td;

    // helper for fetchNext
    private int counter;
    private boolean called;

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
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.counter = -1;
        this.called = false;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
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
        this.counter = -1;
        this.called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.counter = 0;
        this.child.rewind();
        this.called = false;
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
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.called) {
            return null;
        }
        this.called = true;
        while (this.child.hasNext()) {

            try {
                Database.getBufferPool().insertTuple(this.tid, dbFile.getId(), child.next());
                this.counter++;
            } catch (IOException e) {
                throw new DbException("insertTuple failed");
            }
        }
        Tuple tu = new Tuple(this.td);
        tu.setField(0, new IntField(this.counter));
        return tu;
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
