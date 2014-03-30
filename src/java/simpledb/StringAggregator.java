package simpledb;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield; 
    Type gbfieldtype; 
    int afield; 
    Op what;
    private TupleDesc td;
    private HashMap<Field, Integer> groups;
    private String gbfieldName;
    private String afieldName;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
    	// TupleDesc td
    	Type[] fieldTypes;
    	String[] fieldNames;
        if(gbfield == NO_GROUPING){
        	fieldTypes = new Type[1];
        	fieldNames = new String[1];
    		fieldTypes[0] = Type.INT_TYPE;
    		fieldNames[0] = afieldName;
        } else {
        	fieldTypes = new Type[2];
        	fieldNames = new String[2];
    		fieldTypes[0] = this.gbfieldtype;
    		fieldTypes[1] = Type.INT_TYPE;
    		fieldNames[0] = this.gbfieldName;
    		fieldNames[1] = this.afieldName;   
        }
        this.td = new TupleDesc(fieldTypes, fieldNames);
        
        //if(!this.what.toString().equals(Op.COUNT))
        if(this.what.toString() != "count")
        	throw new IllegalArgumentException();
        this.groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key = null;
    	if(this.gbfieldtype != null){
    		key = tup.getField(this.gbfield);
    		this.gbfieldName = tup.getTupleDesc().getFieldName(gbfield);
    	}
    	this.afieldName = tup.getTupleDesc().getFieldName(afield);
    	if(!this.groups.containsKey(key)){
    		this.groups.put(key, 1);
    	} else {
    		this.groups.put(key, this.groups.get(key) + 1);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
		Field count = null;
		Tuple newTup = new Tuple(this.td);
    	ArrayList<Tuple> aggTuples = new ArrayList<Tuple>();
    	
    	if(this.gbfieldtype == null){
    		count = new IntField(this.groups.get(null));
    		newTup.setField(0, count);
    		aggTuples.add(newTup);
    		return new TupleIterator(this.td, aggTuples);
    	}else{
    		Set<Field> keys = this.groups.keySet();
    		for(Field field : keys){
    			newTup = new Tuple(this.td);
    			newTup.setField(0, field);
    			count = new IntField(this.groups.get(field));
    			newTup.setField(1, count);
    			aggTuples.add(newTup);
    		}  
    		return new TupleIterator(this.td, aggTuples);
    	}
	}
    
	public TupleDesc getTupleDesc(){
		return this.td;
	}
}
