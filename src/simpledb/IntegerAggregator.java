package simpledb;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private String gbfieldName;
    private String afieldName;
    private HashMap<Field, Integer> groups;
    private HashMap<Field, Integer> numofeachGroup;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	this.groups = new HashMap<Field, Integer>();
    	this.numofeachGroup = new HashMap<Field, Integer>();
    	
    	// create TupleDesc td
        //ArrayList<Type> fieldTypes = new ArrayList<Type>(2);
        //ArrayList<String> fieldNames = new ArrayList<String>(2); 
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
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key = null;
    	if(this.gbfield != NO_GROUPING){
    		key = tup.getField(this.gbfield);
    		this.gbfieldName = tup.getTupleDesc().getFieldName(gbfield);	
    	} 
    	
    	IntField afield = (IntField) tup.getField(this.afield);
    	if(this.groups.containsKey(key)){
    		Integer value = this.groups.get(key);
    		this.numofeachGroup.put(key, this.numofeachGroup.get(key) + 1);
	    	switch(this.what) {
	    		case MIN: 
	    			this.groups.put(key, Math.min(afield.getValue(), value));
	    			break;
	    		case MAX:
	    			this.groups.put(key, Math.max(afield.getValue(), value));
	    			break;
	    		case SUM: 
	    			this.groups.put(key, afield.getValue() + value);
	    			break;
	    		case AVG: 
	    			//this.groups.put(key, new Integer((int) ((int)(afield.getValue() + value)/2.0)));
	    			this.groups.put(key, afield.getValue() + value);
	    			break;   
	    		case COUNT: 
	    			this.groups.put(key, this.numofeachGroup.get(key));
	    			break;
	    		default: System.out.println("Error: Op what value invalid!");
	    	}
    	}else{
	    		this.groups.put(key, afield.getValue());
	    		this.numofeachGroup.put(key, 1);
	    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
		IntField aggVal = null;
		Integer count = null;
		Integer value = null;
		Tuple newTup = new Tuple(this.td);
    	ArrayList<Tuple> aggTuples = new ArrayList<Tuple>();
    	
    	if(this.gbfieldtype == null){
    		count = this.numofeachGroup.get(null);
			value = this.groups.get(null);
			if(this.what.toString().equals("avg")){
				aggVal = new IntField(value/count);
			} else {
				aggVal = new IntField(value);
			}
    		newTup.setField(0, aggVal);
    		aggTuples.add(newTup);
    		return new TupleIterator(this.td, aggTuples);
    	}else {
    		Set<Field> keys = this.groups.keySet();
    		for(Field field : keys){
    			newTup = new Tuple(this.td);
    			newTup.setField(0, field);
    			count = this.numofeachGroup.get(field);
    			value = this.groups.get(field);
    			if(this.what.toString().equals("avg")){
    				aggVal = new IntField(value/count);
    			} else {
    				aggVal = new IntField(value);
    			}
    			newTup.setField(1, aggVal);
    			aggTuples.add(newTup);
    		}  
    		return new TupleIterator(this.td, aggTuples);
    	}
    }
    
	public TupleDesc getTupleDesc(){
		return this.td;
	}

}
