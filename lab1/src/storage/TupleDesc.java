package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private static final long serialVersionUID = 1L;
    //这里之前没先生成对象，一直报错，呜呜呜！！！
    private ArrayList<TDItem> tditems = new ArrayList<TDItem>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof TDItem) {
                return fieldType.equals(((TDItem) obj).fieldType);
            }
            return false;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field tditems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tditems.iterator();
    }


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
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        for ( int i = 0; i < typeAr.length; i++ )
        {
            TDItem tditem = new TDItem(typeAr[i], fieldAr[i]);
            tditems.add(tditem);
        }
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
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tditems.size();
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
        if ( i >= this.tditems.size() )
            throw new NoSuchElementException();

        return this.tditems.get(i).fieldName;
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
        if ( i >= tditems.size() )
            throw new NoSuchElementException();

        return tditems.get(i).fieldType;
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
        int i;
        for ( i = 0; i < this.tditems.size(); i++ )
        {
            String fieldname = this.tditems.get(i).fieldName;
            if ( fieldname != null && fieldname.equals(name) )
            {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tditems) {
            size += item.fieldType.getLen();
        }
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
        // // There are occupying the td1, maybe something wrong
        // note : need to return a new TupleDescs
        // for ( int i = 0; i < td2.tditems.size(); i++ )
        // {
        //     td1.tditems.add(td2.tditems.get(i));
        // }
        // return td1;
        int td1len = td1.numFields();
        int td2len = td2.numFields();
        int totoal = td1len + td2len;

        Type[] types = new Type[totoal];
        String[] fields = new String[totoal];
        for ( int i = 0; i < td1len; i++ )
        {
            types[i] = td1.tditems.get(i).fieldType;
            fields[i] = td1.tditems.get(i).fieldName;
        }
        for ( int i = td1len; i < totoal; i++ )
        {
            types[i] = td2.tditems.get(i-td1len).fieldType;
            fields[i] = td2.tditems.get(i-td1len).fieldName;
        }

        return new TupleDesc(types, fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if ( o == null && tditems == null )
            return true;
        else if ( o != null && o instanceof TupleDesc && tditems.equals(((TupleDesc)o).tditems))
            return true;
        else
            return false;
        // if (!(o instanceof  TupleDesc)) {
        //     return false;
        // }

        // TupleDesc other = (TupleDesc) o;
        // if (other.getSize() != this.getSize() || other.numFields() != this.numFields()) {
        //     return false;
        // }

        // for (int i = 0; i < this.numFields(); i++) {
        //     if (!this.getFieldType(i).equals(other.getFieldType(i))) {
        //         return false;
        //     }
        // }

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
        String fields = new String();
        int i;
        for ( i = 0; i < this.tditems.size() - 1; i++ )
        {
            fields += this.tditems.get(i).fieldType + "[" + i + "](" +
                this.tditems.get(i).fieldName + "[" + i + "]),";
        }
        fields += this.tditems.get(i).fieldType + "[" + i + "](" +
                this.tditems.get(i).fieldName + "[" + i + "])";

        return fields;
    }
}
