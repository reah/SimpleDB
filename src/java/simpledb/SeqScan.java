package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private int tableid;
    private String tableAlias;
    private TransactionId tid;
    DbFileIterator fileIt;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.tid = tid;
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	this.fileIt = null;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	this.tableAlias = tableAlias;
    	this.tableid = tableid;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }
        
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	fileIt = Database.getCatalog().getDbFile(tableid).iterator(tid);
    	fileIt.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        Iterator<TDItem> tdIter = td.iterator();
        int size = td.numFields();
        Type[] typeAr = new Type[size];
        String[] fieldAr = new String[size];
        
        String aliasString = this.tableAlias;
        
        TDItem item;
        Type fieldType;
        String fieldName;
        int count = 0;
        
        if(aliasString == null) {
        	aliasString = "null";
        }
//      for (int i = 0; i < size; i++){
//    	item = tdIter.next();
//    	fieldType = item.fieldType;
//    	fieldName = item.fieldName;
        
        while(tdIter.hasNext()){
        	item = tdIter.next();
        	fieldType = item.fieldType;
        	fieldName = item.fieldName;
        	if(fieldName == null){
        		fieldName = "null";
        	}
        	typeAr[count] = fieldType;
        	fieldAr[count] = aliasString + "." + fieldName; //"null.null case may occur"
        	count++;        	
        }
        return new TupleDesc(typeAr,fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(fileIt == null) {
        	return false;
        }
        return fileIt.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(fileIt == null){
        	throw new NoSuchElementException("No Such Element");
        } 
        Tuple temp = fileIt.next();
        if(temp == null){
        	throw new NoSuchElementException("No Next Element");
        }
        else {
        	return temp;
        }
    }

    public void close() {
        // some code goes here
    	fileIt = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	fileIt.close();
    	fileIt.open();
    }
}
