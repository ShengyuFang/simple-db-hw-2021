package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;

    private int min;

    private int max;

    private int width;

    private int[] bucketArray;

    private int count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.bucketArray = new int[buckets];
        // max - min + 1个数填入buckets个桶
        // Math.ceil((max - min + 1) / buckets) = (max - min + 1 + buckets -1)/buckets
        this.width = Math.max((max - min + buckets) / buckets, 1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
//        int index = (v-min)/((max - min + 1)/buckets);
        int index = getBucketIndex(v);
        bucketArray[index]++;
        ++count;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int index = getBucketIndex(v);
        if(op.equals(Predicate.Op.EQUALS) || op.equals(Predicate.Op.LIKE)) {
            if(v > max || v < min) {
                return 0.0;
            }
            return ((double) bucketArray[index] / count);
        } else if (op.equals(Predicate.Op.GREATER_THAN)) {
            if(v > max) {
                return 0.0;
            }
            if(v <= min) {
                return 1.0;
            }
            int sum = 0;
            for(int i = index + 1; i < buckets; ++i) {
                sum += bucketArray[i];
            }
            return (double) (sum + ((index + 1) * width + min - 1 - v)*bucketArray[index]) / count;
        } else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if(v > max) {
                return 0.0;
            }
            if(v <= min) {
                return 1.0;
            }
            int sum = 0;
            for(int i = index + 1; i < buckets; ++i) {
                sum += bucketArray[i];
            }
            return (double) (sum + ((index + 1) * width + min - v)*bucketArray[index]) / count;
        } else if (op.equals(Predicate.Op.LESS_THAN)) {
            if(v < min) {
                return 0.0;
            }
            if(v >= max) {
                return 1.0;
            }
            int sum = 0;
            for(int i = 0; i < index; ++i) {
                sum += bucketArray[i];
            }
            return (double)(sum + (v - (index * width + min))*bucketArray[index]) / count;
        } else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            if(v < min) {
                return 0.0;
            }
            if(v >= max) {
                return 1.0;
            }
            int sum = 0;
            for(int i = 0; i < index; ++i) {
                sum += bucketArray[i];
            }
            return (double)(sum + (v - (index * width + min) + 1)*bucketArray[index]) / count;
        } else if (op.equals(Predicate.Op.NOT_EQUALS)) {
            if(v > max || v < min) {
                return 1;
            }
            return ((double) (count - bucketArray[index])) / count;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    //Todo
    public double avgSelectivity()
    {
        // some code goes here
        return (double) count / buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(bucketArray);
    }

    private int getBucketIndex(int v) {
        return (v - min) / width;
    }
}
