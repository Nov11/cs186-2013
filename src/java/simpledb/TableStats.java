package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();
    private int tableId;
    private int tupleCount;
    private int ioCostPerPage;
    private Map<Integer, Object> hash;
    private List<Type> typeList;
    private TupleDesc tupleDesc;


    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.hash = new HashMap<>();
        this.typeList = new ArrayList<>();
        this.tupleCount = 0;
        DbFile dbFile = Database.getCatalog().getDbFile(tableid);
        tupleDesc = dbFile.getTupleDesc();

        int index = 0;
        Iterator<TupleDesc.TDItem> iterator = tupleDesc.iterator();
        while (iterator.hasNext()) {
            TupleDesc.TDItem item = iterator.next();
            if (item.fieldType == Type.STRING_TYPE) {
                typeList.add(Type.STRING_TYPE);
            } else {
                typeList.add(Type.INT_TYPE);
            }
            hash.put(index, null);
            index++;
        }
        Map<Integer, List<Integer>> tmpMinMax = new HashMap<>();
        Map<Integer, List<Integer>> tmpIntValue = new HashMap<>();
        TransactionId tid = new TransactionId();
        HeapFile heapFile = (HeapFile) dbFile;
        DbFileIterator dbFileIterator = heapFile.iterator(tid);
        try {
            dbFileIterator.open();
            while (dbFileIterator.hasNext()) {
                Tuple tuple = dbFileIterator.next();
                for (int i = 0; i < typeList.size(); i++) {
                    if (typeList.get(i) == Type.STRING_TYPE) {
                        if (hash.get(i) == null) {
                            hash.put(i, new StringHistogram(NUM_HIST_BINS));
                        }
                        StringHistogram stringHistogram = (StringHistogram) hash.get(i);
                        StringField stringField = (StringField) tuple.getField(i);
                        stringHistogram.addValue(stringField.getValue());
                    } else {
                        if (tmpMinMax.get(i) == null) {
                            List<Integer> innerTmp = new ArrayList<>();
                            innerTmp.add(Integer.MAX_VALUE);
                            innerTmp.add(Integer.MIN_VALUE);
                            tmpMinMax.put(i, innerTmp);
                        }
                        List<Integer> innerList = tmpMinMax.get(i);
                        IntField intField = (IntField) tuple.getField(i);
                        innerList.set(0, Math.min(innerList.get(0), intField.getValue()));
                        innerList.set(1, Math.max(innerList.get(1), intField.getValue()));
                        List<Integer> intValue = tmpIntValue.computeIfAbsent(i, k -> new ArrayList<>());
                        intValue.add(intField.getValue());
                    }
                }
                tupleCount++;
            }

            for (Map.Entry<Integer, List<Integer>> entry : tmpMinMax.entrySet()) {
                Integer fieldId = entry.getKey();
                List<Integer> minMax = entry.getValue();
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minMax.get(0), minMax.get(1));
                hash.put(fieldId, intHistogram);
                List<Integer> values = tmpIntValue.get(fieldId);
                for (Integer v : values) {
                    intHistogram.addValue(v);
                }
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        HeapFile heapFile = (HeapFile) dbFile;
        return heapFile.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type type = typeList.get(field);
        if (type == Type.STRING_TYPE) {
            StringHistogram stringHistogram = (StringHistogram) hash.get(field);
            StringField stringField = (StringField) constant;
            return stringHistogram.estimateSelectivity(op, stringField.getValue());
        } else {
            IntHistogram intHistogram = (IntHistogram) hash.get(field);
            IntField intField = (IntField) constant;
            return intHistogram.estimateSelectivity(op, intField.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return tupleCount;
    }

}
