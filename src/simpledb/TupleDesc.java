package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> fields;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.fields.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr){
        // some code goes here
        try{
          int len = typeAr.length;
          len = fieldAr.length;
        }catch(NullPointerException e){
          System.out.println("typeAr[] or fieldAr[] is null");
          System.out.println(e);
        }

        //check typeAr for at least one entry
        if(typeAr.length == 0 || (typeAr.length != fieldAr.length))
	     System.out.println("typeAr[] has length 0 or typeAr[] and fieldAr[] are not same length");

        //add TDItems to private ArrayList: fields
        this.fields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++)
          this.fields.add(new TDItem(typeAr[i], fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        try{
          int len = typeAr.length;
        }catch(NullPointerException e){
          System.out.println("typeAr[] is null");
          System.out.println(e);
        }

       //check typeAr for at least one entry
        if(typeAr.length == 0)
          System.out.println("typeAr[] has length 0");

        //add TDItems to private ArrayList: fields
        fields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++)
          fields.add(new TDItem(typeAr[i], null));
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= this.fields.size())
          throw new NoSuchElementException(i + " is not a valid field reference");

        return this.fields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= this.fields.size())
          throw new NoSuchElementException(i + " is not a valid field reference");

        return this.fields.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i = 0; i < this.fields.size(); i++){
          if(this.fields.get(i).fieldName != null && this.fields.get(i).fieldName.equals(name))
            return i;
        }
        throw new NoSuchElementException(" No field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int i = 0; i < this.numFields(); i++)
            size += this.fields.get(i).fieldType.getLen();
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int size = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[size];
        String[] fieldAr = new String[size];

        int i;
        for(i = 0; i < td1.numFields(); i++){
          typeAr[i] = td1.fields.get(i).fieldType;
          fieldAr[i] = td1.fields.get(i).fieldName;
        }

        for(int j = 0; j < td2.numFields(); j++){
          typeAr[i+j] = td2.fields.get(j).fieldType;
          fieldAr[i+j] = td2.fields.get(j).fieldName;
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc) || (((TupleDesc)o).numFields() != this.numFields()))
          return false;

        for(int i = 0; i < ((TupleDesc)o).numFields(); i++){
          if(this.fields.get(i).fieldType != ((TupleDesc)o).fields.get(i).fieldType)
            return false;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer descriptor = new StringBuffer();
        for(int i = 0; i < this.numFields(); i++)
          descriptor.append(this.fields.get(i).fieldType.toString() + "(" + this.fields.get(i).fieldName + ") , ");
        return descriptor.toString();
    }
}
