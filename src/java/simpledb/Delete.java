package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private boolean deleteStatus = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
        this.child = child;
        //init a td
        Type[] typeAr = new Type[1];
        String[] fieldAr = new String[1];
        typeAr[0] = Type.INT_TYPE;
        fieldAr[0] = "count";
        this.td = new TupleDesc(typeAr, fieldAr);
        
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	//if (deleteStatus) {
    	//	throw new DbException("delete operator is already open");
    	//}
    	//else{
    		super.open();
    		child.open();
    		deleteStatus = false;
    	//}
    }

    public void close() {
        // some code goes here
    	//if (deleteStatus) {
    		super.close();
    		child.close();
    	//} else {
    	//	System.out.println("delete operator is already closed");
    	//}
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		this.close();
    		this.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (!deleteStatus){
    		deleteStatus = true;
    		int fieldCount = 0;
            BufferPool bufferPool = Database.getBufferPool();
            Tuple delete;
            while(child.hasNext()){
                delete = child.next();
                try{
                	bufferPool.deleteTuple(tid, delete);
                }catch(Exception e){
                	//System.out.println(e);
                }
                fieldCount++;
            }
            Tuple nextTuple = new Tuple(td);
            IntField intField  = new IntField(fieldCount);
            nextTuple.setField(0, intField);
            return nextTuple;
        } 
    	else{
    		return null;
    	}
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child = children[0];
    }

}
