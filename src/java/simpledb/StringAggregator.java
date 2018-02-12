package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupFieldId;
    private Type groupFieldType;
    private int aggregateFieldId;
    private Op operation;
    private Map<Field, List<String>> hash;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupFieldId = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregateFieldId = afield;
        this.operation = what;
        assert what == Op.COUNT;
        hash = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = null;
        if (groupFieldId != NO_GROUPING) {
            field = tup.getField(groupFieldId);
        }
        List<String> list = hash.computeIfAbsent(field, k -> new ArrayList<>());
        assert tup.getField(aggregateFieldId) instanceof StringField;
        StringField stringField = (StringField) tup.getField(aggregateFieldId);
        list.add(stringField.getValue());
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for proj2");
        TupleDesc tupleDesc = null;
        if (groupFieldId == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{groupFieldType, Type.INT_TYPE});
        }
        return new StringAggregaterIter(this, tupleDesc);
    }

    static class StringAggregaterIter implements DbIterator {
        private boolean open;
        private StringAggregator stringAggregator;
        private TupleDesc tupleDesc;
        private Iterator<Map.Entry<Field, List<String>>> iterator;

        StringAggregaterIter(StringAggregator stringAggregator, TupleDesc tupleDesc) {
            this.stringAggregator = stringAggregator;
            this.tupleDesc = tupleDesc;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.open = true;
            iterator = stringAggregator.hash.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.open)
                throw new IllegalStateException("Operator not yet open");
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple result = new Tuple(tupleDesc);
            Map.Entry<Field, List<String>> item = iterator.next();
            Field field = item.getKey();
            List<String> list = item.getValue();
            if (stringAggregator.groupFieldId != NO_GROUPING) {
                result.setField(0, field);
                result.setField(1, new IntField(list.size()));
            } else {
                result.setField(0, new IntField(list.size()));
            }
            return result;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = stringAggregator.hash.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            this.open = false;
        }
    }
}
