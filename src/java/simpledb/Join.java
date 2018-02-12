package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate joinPredicate;
    private DbIterator child1;
    private DbIterator child2;
    private Tuple outer;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        // some code goes here
        TupleDesc tupleDesc = child1.getTupleDesc();
        int idx = joinPredicate.getField1();
        return tupleDesc.getFieldName(idx);
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        TupleDesc tupleDesc = child2.getTupleDesc();
        int idx = joinPredicate.getField2();
        return tupleDesc.getFieldName(idx);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc desc1 = child1.getTupleDesc();
        TupleDesc desc2 = child2.getTupleDesc();
        return TupleDesc.merge(desc1, desc2);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        outer = null;
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        outer = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        outer = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (outer == null) {
            if (!child1.hasNext()) {
                return null;
            }
            outer = child1.next();
        }
        while (outer != null) {
            while (child2.hasNext()) {
                Tuple tuple2 = child2.next();
                if (joinPredicate.filter(outer, tuple2)) {
                    Tuple result = new Tuple(getTupleDesc());
                    Iterator<Field> ii = outer.fields();
                    int idx = 0;
                    while (ii.hasNext()) {
                        result.setField(idx++, ii.next());
                    }
                    ii = tuple2.fields();
                    while (ii.hasNext()) {
                        result.setField(idx++, ii.next());
                    }
                    return result;
                }
            }

            child2.rewind();
            if(child1.hasNext()){
                outer = child1.next();
            }else {
                outer = null;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }

}
