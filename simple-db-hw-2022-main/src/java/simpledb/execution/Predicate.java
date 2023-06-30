package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 * 过滤操作，相当于查询语句里的where子句
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {//enum枚举类
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
        //等于，大于，小于，小于等于，大于等于，不等于

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index(有效整数运算索引)
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {//根据比较返回相应的符号表示
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private  int fields;
    private  Op ops;
    private Field operands;
    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against.(tuple中哪一列数据。)
     * @param op      operation to use for comparison（算子）
     * @param operand field value to compare passed in tuples to(要比较的字段)
     */
    public Predicate(int field, Op op, Field operand) {
        // TODO: some code goes here
        this.fields=field;
        this.ops=op;
        this.operands=operand;
    }

    /**
     * @return the field number（tuple中哪一列数据）
     */
    public int getField() {
        // TODO: some code goes here
        return fields;
    }

    /**
     * @return the operator（返回算子）
     */
    public Op getOp() {
        // TODO: some code goes here
        return ops;
    }

    /**
     * @return the operand（要比较的字段）例如where id>=100 ,其中100即为operand
     */
    public Field getOperand() {
        // TODO: some code goes here
        return operands;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against//参与比较的元组
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {//首先获取元组相应列的值，利用接口函数Filed的compare函数进行过滤返回
        // TODO: some code goes here
        Field get_operand=t.getField(fields);
        return get_operand.compare(ops,operands);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // TODO: some code goes here
        return String.format("f=%d op=%s operand=%s",fields,ops,operands);
    }
}
