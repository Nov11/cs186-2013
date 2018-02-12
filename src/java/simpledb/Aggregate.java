package simpledb;

import java.util.*;

import static simpledb.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int aggregateFieldId;
    private int groupingFieldId;
    private Aggregator.Op operation;
    private DbIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggregateFieldId = afield;
        this.groupingFieldId = gfield;
        this.operation = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupingFieldId;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (groupingFieldId == NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(groupingFieldId);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggregateFieldId;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(aggregateFieldId);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return operation;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        TupleDesc inputDesc = child.getTupleDesc();
        Type aggregateType = inputDesc.getFieldType(aggregateFieldId);
        Aggregator aggregator = null;
        if (aggregateType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupingFieldId, inputDesc.getFieldType(groupingFieldId), aggregateFieldId, operation);
        } else if (aggregateType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(groupingFieldId, inputDesc.getFieldType(groupingFieldId), aggregateFieldId, operation);
        } else {
            throw new DbException("not supported yet");
        }
        while (child.hasNext()) {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }
        iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator.rewind();
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
        // some code goes here
        TupleDesc result = null;
        if (groupingFieldId == NO_GROUPING) {
            result = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            result = new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(groupingFieldId), Type.INT_TYPE});
        }
        return result;
    }

    public void close() {
        // some code goes here
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
