package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {
	//Member Variables
	private int numTuples;
	private int ioCostPerPage;
	private TupleDesc td;
	private HeapFile file;
	//Map field name to minimum value
	private Map<String, Integer> minMap;
	//Map field name to maximum value
	private Map<String, Integer> maxMap;
	//Map field name to histogram int values
	private Map<String, IntHistogram> histValues;
	//Map field name to histogram string fields
	private Map<String, StringHistogram> histNames;

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
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
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
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
    	this.numTuples = 0;
    	this.ioCostPerPage = ioCostPerPage;
    	this.file = (HeapFile) Database.getCatalog().getDbFile(tableid);
    	this.td = file.getTupleDesc();
    	this.maxMap = new HashMap<String, Integer>();
    	this.minMap = new HashMap<String, Integer>();
    	this.histValues = new HashMap<String, IntHistogram>();
    	this.histNames = new HashMap<String, StringHistogram>();
    	this.fillMaps();
    	this.createHistograms();
    }
    
    /**
     * Helper method for computation of helper statistics, specifically: tuple count and min/max values for each field of file
     */
    private void fillMaps() {
    	//TODO: figure out what transactionId is needed
    	TransactionId tId = null;
    	DbFileIterator iterator = this.file.iterator(tId);
	    try {
			iterator.open();
			while(iterator.hasNext()){
	    		Tuple tuple = iterator.next();
	    		// fill maps for each field in tupledesc td
	    		for(int i = 0; i < this.td.numFields(); i++){
	    			if(this.td.getFieldType(i).equals(Type.INT_TYPE)){
	    				IntField field = (IntField)tuple.getField(i);
	    				String fieldName = this.td.getFieldName(i);
	    				// fill maxMap
	    				if(this.maxMap.containsKey(fieldName)){
	    					if(field.getValue() > this.maxMap.get(fieldName)){
	    						this.maxMap.put(fieldName, field.getValue());
	    					}
	    				} else{
	    					this.maxMap.put(fieldName, field.getValue());
	    				}
	    				// fill minMap
	    				if(this.minMap.containsKey(fieldName)){
	    					if(field.getValue() < this.minMap.get(fieldName)){
	    						this.minMap.put(fieldName, field.getValue());
	    					}
	    				} else {
	    					this.minMap.put(fieldName, field.getValue());
	    				}			
	    			}else{
	    				throw new Exception("Field is not of INT_TYPE");
	    			}
	    		}
			this.numTuples += 1;
	    	}
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("Unknown Error thrown: possibly due to field not being of INT_TYPE");
		}
	    iterator.close();		
    }
    
    /**
     * Helper method to populate histogram Map with corresponding histograms and fill values from given table
     */
    private void createHistograms() {
    	TransactionId tId = null;
    	DbFileIterator iterator = this.file.iterator(tId);
    	try {
			iterator.open();
			while(iterator.hasNext()){
	    		Tuple tuple = iterator.next();
	    		// Update histogram corresponding to each field in tuple
		    	for(int i = 0; i < this.td.numFields(); i++){
		    		String fieldName = this.td.getFieldName(i);
		    		if(this.td.getFieldType(i).equals(Type.STRING_TYPE)){
		    			StringField field = (StringField)tuple.getField(i);
		    			String value = field.getValue();
		    			if(histNames.containsKey(fieldName))
		    				histNames.get(fieldName).addValue(value);
		    			else
		    				histNames.put(fieldName, new StringHistogram(NUM_HIST_BINS));
		    		} else {
		    			IntField field = (IntField)tuple.getField(i);
			    		int value = field.getValue();
			    		
		    			if(histValues.containsKey(fieldName))
		    				histValues.get(fieldName).addValue(value);
		    			else
		    				histValues.put(fieldName, new IntHistogram(NUM_HIST_BINS, minMap.get(fieldName), maxMap.get(fieldName)));
		    		}
		    	}
	    	}
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
    	iterator.close();		
	}

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.ioCostPerPage * this.file.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor * this.numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if(td.getFieldType(field).equals(Type.INT_TYPE))
        	return histValues.get(td.getFieldName(field)).estimateSelectivity(op, ((IntField)constant).getValue());
        else 
        	return histNames.get(td.getFieldName(field)).estimateSelectivity(op, ((StringField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
