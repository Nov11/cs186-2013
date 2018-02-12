package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupingField;
    private Type groupingFieldType;
    private int aggregateFieldId;
    private Op operation;
    private Map<Field, List<Integer>> hash;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupingField = gbfield;
        this.groupingFieldType = gbfieldtype;
        this.aggregateFieldId = afield;
        this.operation = what;
        hash = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        List<Integer> value = null;
        if (groupingField == NO_GROUPING) {
            value = hash.computeIfAbsent(null, k -> new ArrayList<>());
        } else {
            Field field = tup.getField(groupingField);
            value = hash.computeIfAbsent(field, k -> new ArrayList<>());
        }
        assert tup.getField(aggregateFieldId) instanceof IntField;
        IntField afield = (IntField) tup.getField(aggregateFieldId);
        value.add(afield.getValue());
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new
//                UnsupportedOperationException("please implement me for proj2");
        TupleDesc tupleDesc = null;
        if (groupingField == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{groupingFieldType, Type.INT_TYPE});
        }
        return new IntAggregateIter(this, tupleDesc);
    }

    static class IntAggregateIter implements DbIterator {

        private boolean open = false;
        private IntegerAggregator integerAggregator;
        private TupleDesc desc;
        private Iterator<Map.Entry<Field, List<Integer>>> iterator;

        public IntAggregateIter(IntegerAggregator integerAggregator, TupleDesc desc) {
            this.integerAggregator = integerAggregator;
            this.desc = desc;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            iterator = integerAggregator.hash.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.open)
                throw new IllegalStateException("Operator not yet open");
            return iterator.hasNext();
        }

        private Integer compute(Op op, List<Integer> list) {
            assert list.size() > 0;
            Integer result = 0;
            switch (op) {
                case AVG: {
                    result = 0;
                    for (Integer item : list) {
                        result += item;
                    }
                    result /= list.size();
                }
                break;
                case MIN: {
                    result = list.get(0);
                    for (Integer item : list) {
                        result = Math.min(result, item);
                    }

                }
                break;
                case MAX: {
                    result = list.get(0);
                    for (Integer item : list) {
                        result = Math.max(result, item);
                    }

                }
                break;
                case COUNT: {
                    result = list.size();
                }
                break;
                case SUM: {
                    result = 0;
                    for (Integer item : list) {
                        result += item;
                    }
                }
                break;
                default:
                    assert false;
            }
            return result;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<Field, List<Integer>> entry = iterator.next();
            Tuple result = new Tuple(desc);
            Integer ret = compute(integerAggregator.operation, entry.getValue());
            if (integerAggregator.groupingField == NO_GROUPING) {
                result.setField(0, new IntField(ret));
            } else {
                result.setField(0, entry.getKey());
                result.setField(1, new IntField(ret));
            }
            return result;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = integerAggregator.hash.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return desc;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
