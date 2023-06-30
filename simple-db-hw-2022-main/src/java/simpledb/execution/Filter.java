package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {//继承Operator

    private static final long serialVersionUID = 1L;
    private final Predicate p;//用于筛选元组的谓词
    private  OpIterator child;//参与筛选元组的迭代器对象
    private final TupleDesc tupleDesc;//参与筛选的元组的元信息
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with//用于筛选元组的谓词
     * @param child The child operator//参与筛选的元组
     */
    public Filter(Predicate p, OpIterator child) {
        // TODO: some code goes here
        this.p=p;
        this.child=child;
        this.tupleDesc=child.getTupleDesc();

    }

    public Predicate getPredicate() {//获取筛选的条件
        // TODO: some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {//获取元组元信息
        // TODO: some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {//获取下一个元组
        // TODO: some code goes here
        while (child.hasNext())//遍历所有的元组
        {
            Tuple tuple=child.next();//获取下一个元组
            if(p.filter(tuple))//判断是否符合筛选条件
            {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {//返回迭代器
        // TODO: some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.child=children[0];
    }

}
