package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int buckets;
	private int min;
	private int max;
	private int[] histogram;
	private int width;
	private int lastBucketWidth;
	private int count;
	private int belowZero;
	
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
    	this.belowZero = 0;
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.histogram = new int[buckets];
    	if(min < 0){
    		this.belowZero = min * -1;
    		this.min = 0;
    		this.max = max + this.belowZero;
    	}
    	this.width = (int)Math.ceil(((double)(max - min + 1))/buckets);
    	this.lastBucketWidth = this.max - (min + this.width * (this.buckets - 1)) + 1;
    	this.count = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	this.count++;
    	v += this.belowZero;
    	int position = (v - this.min)/this.width;
    	this.histogram[position] += 1;
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
    	v += this.belowZero;
    	int position = (v - this.min)/this.width;
    	int width = this.width;
    	if(position == this.buckets - 1){
    		width = this.lastBucketWidth;
    	}
    	double estimate = 0;
        switch(op){
	        case EQUALS: 
	        	if(v < this.min || v > this.max)
	        		estimate = 0;
	        	else
	        		estimate = (((double)this.histogram[position])/width)/this.count;
			break;
			case GREATER_THAN:
				estimate = this.greaterThan(v, width, position);
			break;
			case LESS_THAN: 
				estimate = this.lessThan(v, width, position);
			break;
			case LESS_THAN_OR_EQ: 
				estimate = this.lessThan(v + 1, width, position);
			break;   
			case GREATER_THAN_OR_EQ: 
				estimate = this.greaterThan(v - 1, width, position);
			break;  
			case NOT_EQUALS: 
	        	if(v>=min && v<=max)
	        		estimate = 1 - (((double)this.histogram[position])/width)/this.count;
	        	else
	        		estimate = 1;
			break;  
        }
        return estimate;    
    }
    
    /*
     * Helper method for GREATER_THAN/GREATER_THAN_OR_EQ estimation
     */
    
    public double greaterThan(int v, int width, int position){
    	double estimate = 0;
    	double b_f;
    	double b_p;
    	int b_right;
    	if(v < this.min){
			estimate = 1;
		} else if (v > this.max){
			estimate = 0;
		} else {
			b_f = ((double) this.histogram[position])/this.count;
			b_right = this.min + this.width * (position + 1);
			if(position == this.buckets - 1){
				b_right = this.max;
			}
			b_p = ((double)(b_right - v))/width;
			estimate = b_p * b_f;
			for(int i = position + 1; i < this.buckets; i++){
				estimate += ((double)this.histogram[i])/this.count;
			}
		}
    	return estimate;
    }
    
    /*
     * Helper method for LESS_THAN/LESS_THAN_OR_EQ estimation
     */
    
    public double lessThan(int v, int width, int position){
    	double estimate = 0;
    	double b_f;
    	double b_p;
    	int b_left;
    	if(v < this.min){
			estimate = 0;
		} else if (v > this.max){
			estimate = 1;
		} else {
			b_f = ((double) this.histogram[position])/this.count;
			b_left = min + this.width * position;
			b_p = ((double)(v - b_left))/width;
			estimate = b_p * b_f;
			for(int i = position - 1; i >= 0; i--){
				estimate += ((double)this.histogram[i])/this.count;
			}
		}
    	return estimate;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
