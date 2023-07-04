package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 * 采用的是等宽直方图
 */
public class IntHistogram {

    private int [] bucket;//记录数
    private int max;
    private  int min;
    private int nup;
    private double w;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 此 IntHistogram 应维护它接收的整数值的直方图。
     *  它应该将直方图拆分为“桶”桶。
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 直方图的值将通过“addValue（）”函数一次提供一个。
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *您的实现应使用空间，并且执行时间对于直方图的值数都是恒定的。 例如，不应简单地存储排序列表中看到的每个值。
     * @param buckets The number of buckets to split the input value into.要拆分的桶的数量
     * @param min     The minimum integer value that will ever be passed to this class for histogramming最小值
     * @param max     The maximum integer value that will ever be passed to this class for histogramming最大值
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.bucket=new int [buckets];//默认初始化即为0
        this.min=min;
        this.max=max;
        this.w=Math.max(1,(max - min + 1.0) / buckets);
        this.nup=0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *将值添加到要保留直方图的值集。
     * @param v Value to add to the histogram
     */
    /**
     * 讲v加入到直方图，要确定将v加入到哪个桶里面，首先需要一个函数获取v所加入桶的下标
     * 需要定义函数来获取下标值
     */
    public void addValue(int v) {
        // TODO: some code goes here
        int index=getIndex(v);
        if(v>=min&&v<=max&&index!=-1)
        {
            bucket[index]++;
            this.nup++;
        }
    }
    private int getIndex(int v) {
        int index=(int)((v-min)/w);
        if(index >=0&&index<this.bucket.length)
        {
            return index;
        }
        return -1;
    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 估计此表上特定谓词和操作数的选择性。
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ,GREATER_THAN_OR_EQ,NOT_EQUALS
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // TODO: someop code goes here
        switch (op)
        {
            case GREATER_THAN:
                if(v > max){
                    return 0.0;
                } else if (v <= min){
                    return 1.0;
                } else {
                    int index = getIndex(v);
                    double tuplenum = 0;
                    for(int i = index + 1; i < bucket.length; i++){
                        tuplenum += bucket[i];
                    }
                    // 2 * 4 + 2 - 1 -7
                    tuplenum += (min + (getIndex(v) + 1) * w - 1 - v) *  (1.0 *bucket[index] / w);//?
                    return tuplenum / nup;
                }
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN,v-1);
            case LESS_THAN:
                return 1-estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ,v);
            case EQUALS:
                return 1-estimateSelectivity(Predicate.Op.GREATER_THAN,v)
                        -estimateSelectivity(Predicate.Op.LESS_THAN,v);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);
            case NOT_EQUALS:
                return 1-estimateSelectivity(Predicate.Op.EQUALS,v);
            default:
                throw new UnsupportedOperationException("Op is illegal");
        }
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     *         这不是实现基本连接优化不可或缺的方法。如果要实现更有效的优化，则可能需要它
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        double sum=0.0;
        for (int j : bucket) {
            sum += j/(nup*1.0);
        }
        return sum/(bucket.length*1.0);

    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return String.format("IntHistgram(buckets=%d, min=%d, max=%d",
                bucket.length, min, max);
    }
}
