package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int minValue;
    private int maxValue;
    private List<Long> list;
    private int singleBucketCapacity;
    private long total;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        assert min <= max && buckets > 0;
        this.buckets = buckets;
        this.minValue = min;
        this.maxValue = max;
        list = new ArrayList<>();
        for (int i = 0; i < buckets; i++) {
            list.add(0L);
        }
        long valueCount = max - min + 1;
        this.singleBucketCapacity = (int) Math.ceil(valueCount * 1.0 / buckets);
        total = 0;
    }

    private int bucketIndexZeroBase(int value) {
        return (value - this.minValue) / this.singleBucketCapacity;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        assert v >= minValue && v <= maxValue;
        int idx = bucketIndexZeroBase(v);
        list.set(idx, list.get(idx) + 1L);
        total++;
    }

    private long getHeight(int value) {
        if (value < minValue || value > maxValue) {
            return 0;
        }
        int idx = bucketIndexZeroBase(value);
        return list.get(idx);
    }

    private double greater(boolean eq, int v) {
        double result = 0;
        if (v > maxValue) {
            return result;
        }
        if (v < minValue) {
            return 1;
        }

        int idx = bucketIndexZeroBase(v);
        long count = getHeight(v);
        result = count * 1.0 / total * (1 - (v % singleBucketCapacity + (eq ? 0 : 1)) * 1.0 / singleBucketCapacity);
        for (int i = idx + 1; i < buckets; i++) {
            result += list.get(i) * 1.0 / total;
        }
        return result;
    }

    private double less(boolean eq, int v) {
        double result = 0;
        if (v < minValue) {
            return result;
        }
        if (v > maxValue) {
            return 1;
        }

        int idx = bucketIndexZeroBase(v);
        long count = getHeight(v);
        result = count * 1.0 / total * (1.0 * (v % singleBucketCapacity + (eq ? 1 : 0)) / singleBucketCapacity);
        for (int i = idx - 1; i >= 0; i--) {
            result += list.get(i) * 1.0 / total;
        }
        return result;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double result = -1.0;
        switch (op) {
            case EQUALS: {
                result = getHeight(v) * 1.0 / singleBucketCapacity / total;
                break;
            }
            case NOT_EQUALS: {
                result = 1.0 - getHeight(v) * 1.0 / singleBucketCapacity / total;
                break;
            }
            case GREATER_THAN_OR_EQ: {
                result = greater(true, v);
                break;
            }
            case GREATER_THAN: {
                result = greater(false, v);
                break;
            }
            case LESS_THAN: {
                result = less(false, v);
                break;
            }
            case LESS_THAN_OR_EQ: {
                result = less(true, v);
                break;
            }

            default: {
                result = -1.0;
            }
        }
        // some code goes here
        return result;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(buckets)
                .append(" buckets")
                .append(" min:")
                .append(minValue)
                .append(" max:")
                .append(maxValue)
                .append(" bucketCapacity:")
                .append(singleBucketCapacity)
                .append(" histro:");
        int first = minValue;

        for (int i = 0; i < buckets; i++) {
            int last = first + singleBucketCapacity - 1;
            stringBuilder.append(" [").append(first).append(",").append(last).append("](").append(getHeight(first)).append(")");
            first += singleBucketCapacity;
        }
        return stringBuilder.toString();
    }
}
