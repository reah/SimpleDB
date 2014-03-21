package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p;
    private DbIterator child;
    private boolean filterStatus;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p = p;
    	this.child = child;
    	this.filterStatus = false;
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	if (filterStatus){
    		throw new DbException("Filter is already open");
    	}
    	else{
    		super.open();
    		child.open();
    		filterStatus = true;
    	}
    }

    public void close() {
        // some code goes here
    	if (filterStatus){
    		super.close();
    		child.close();
    		filterStatus = false;
    	}
    	else{
    		System.out.print("Filter is already closed");
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	if (filterStatus){
    		child.rewind();
    	}
    	else{
    		throw new DbException("Filter is closed");
    	}
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	Tuple temp;
    	if (filterStatus){
    		while(child.hasNext()){
    			temp = child.next();
    			if (p.filter(temp)){
    				return temp;
    			}
    		}
    		return null;
    	}
    	else {
    		throw new DbException("Filter is closed");
    	}
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[]
        		{this.child};
    	return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}
