package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private Map<Integer, Integer> histogram;
	private int numTuples;
	private int width;  //  the width of the bucket
	private int min;
	private int max;
	private int buckets;

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
		buckets = Math.min(buckets, max - min + 1);
		this.histogram = new HashMap<>(buckets);
		this.width = (max - min) / buckets + 1;
		this.min = min;
		this.max = max;
		this.buckets = buckets;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 *
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		int k = getBucketKey(v, true);
		this.histogram.put(k, this.histogram.getOrDefault(k, 0) + 1);
		this.numTuples++;  // 增加总的tuple数
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
		// some code goes here
		int k = getBucketKey(v, false);
		if (op == Predicate.Op.EQUALS) {
			return (this.histogram.getOrDefault(k, 0) / (width * 1.0)) / this.numTuples;
		} else if (op == Predicate.Op.NOT_EQUALS) {
			return 1 - (this.histogram.getOrDefault(k, 0) / (width * 1.0)) / this.numTuples;
		} else if (op == Predicate.Op.GREATER_THAN) {
			double t = (this.histogram.getOrDefault(k, 0) / (width * 1.0)) * (k + width - v - 1);
			for (int i = k + width; i < max + width; i += width) {
				t += this.histogram.getOrDefault(i, 0);
			}
			return t / numTuples;
		} else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
			return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
		} else if (op == Predicate.Op.LESS_THAN) {
			double t = (this.histogram.getOrDefault(k, 0) / (width * 1.0)) * (v - k);
			for (int i = min; i < k; ++i) {
				t += this.histogram.getOrDefault(i, 0);
			}
			return t / numTuples;
		} else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
			return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
		} else if (op == Predicate.Op.LIKE) {
			return avgSelectivity();
		}

		throw new IllegalArgumentException();
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
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<Integer, Integer> entry : this.histogram.entrySet()) {
			sb.append("[").append(entry.getKey()).append("-").append(this.width - 1)
					.append("]").append(":").append(entry.getValue()).append(";");
		}
		return sb.toString();
	}

	/**
	 * 获取数据的key
	 * @param v 需要获取key的value
	 * @param isAdd 是否是添加数据 如果是添加需要把key控制在[min, max]
	 */
	private int getBucketKey(int v, boolean isAdd) {
		if (!isAdd) {
			if (v > this.max) {
				return this.max + width;  // 为了实现这个key对应的value为0
			} else if (v < this.min) {
				return this.min - width;  // 为了实现这个key对应的value为0
			}
		}
		for (int i = this.buckets-1; i >= -1; i--) {
			int k = this.min + i * this.width;
			if (v >= k) {
				return k;
			}
		}
		return min;
	}
}
