package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean insertStatus = false;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	this.tid = t;
        this.child = child;
        this.tableId = tableid;
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
    	//if (insertStatus) {
    	//	throw new DbException("insert operator is already open");
    	//}
    	//else{
			insertStatus = false;
    		super.open();
    		child.open();
    	//}
    }

    public void close() {
        // some code goes here
    	//if (insertStatus) {
			super.close();
			child.close();

    	//} else {
    	//	System.out.println("insert operator is already closed");
    	//}
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	//if (insertStatus){
    		this.insertStatus = false;
    		super.close();
    		super.open();
    		child.rewind();
    	//} else{
    	//	throw new DbException("insert operator is closed"); 
    	//}
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
    	if (!insertStatus){
    		insertStatus = true;
    		int fieldCount = 0;
            BufferPool bufferPool = Database.getBufferPool();
            Tuple insert;
            while(child.hasNext()){
                insert = child.next();
                try{
                	bufferPool.insertTuple(tid, tableId, insert);
                }catch(IOException e){
                	System.out.println(e);
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
