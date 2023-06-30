package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.awt.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final Aggregator agg;
    private final OpIterator result;//存放最终结果
    private final TupleDesc td;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 根据字段的类型，您将需要
     *  构造一个 {@link IntegerAggregator} 或 {@link StringAggregator} 来帮助
     *  你与你的 readNext（） 的实现。
     *
     * @param child  The OpIterator that is feeding us tuples.参与聚合的元组
     * @param afield The column over which we are computing an aggregate.(计算聚合的列)
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping（我们对其结果进行分组的列，如果没有分组，则为 -1）
     * @param aop    The aggregation operator to use（要使用的聚合运算符）
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // TODO: some code goes here
        this.child=child;
        this.afield=afield;
        this.gfield=gfield;
        this.aop=aop;
        Type gfieldType=gfield==-1 ? null:this.child.getTupleDesc().getFieldType(gfield);//分组类型
        if(this.child.getTupleDesc().getFieldType(afield)== Type.STRING_TYPE)
        {
            agg=new StringAggregator(gfield,gfieldType,afield,aop);
        }
        else
        {
            agg = new IntegerAggregator(gfield, gfieldType, afield, aop);
        }
        result=agg.iterator();
        List<Type> types=new ArrayList<>();
        List<String> names=new ArrayList<>();
        if(gfield>=0)
        {
            types.add(gfieldType);
            names.add(child.getTupleDesc().getFieldName(gfield));
        }
        types.add(child.getTupleDesc().getFieldType(afield));
        names.add(child.getTupleDesc().getFieldName(afield));
        td=new TupleDesc(types.toArray(new Type[types.size()]),names.toArray(new String[names.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // TODO: some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // TODO: some code goes here
        return this.td.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // TODO: some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // TODO: some code goes here
        return td.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // TODO: some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // TODO: some code goes here

        child.open();
        while (child.hasNext())
        {
            agg.mergeTupleIntoGroup(this.child.next());
        }
        result.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if(result.hasNext())
            return result.next();
        return null;

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        child.rewind();
        result.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        result.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.child=children[0];
    }

}
