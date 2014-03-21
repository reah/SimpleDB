package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    // initialize variables
    private int tupleno;
    private PageId pid;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	this.pid = pid;
    	this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	if(o instanceof RecordId) {
    		//System.out.println(o instanceof RecordId);
    		//System.out.println(this.pid + "     this.pid");
    		//System.out.println(((RecordId) o).getPageId() + "     getPageId");
    		//System.out.println(this.tupleno + "   this.tupleno");
    		//System.out.println(((RecordId) o).tupleno() + "     tupleno()");
    		//System.out.println( (this.pid.equals(((RecordId) o).getPageId())));
    		if((this.pid.equals(((RecordId) o).getPageId())) && (this.tupleno == ((RecordId) o).tupleno())){
    			//System.out.println("1");
    			return true;
    		}
    		//System.out.println("2");
    		return false;
    	}
    	//System.out.println("3");
    	return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return this.pid.hashCode() + this.tupleno();
    }

}
