package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> arrayList = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        Type fieldType;

        /**
         * The name of the field
         */
        String fieldName;

        public TDItem(Type t, String n) {
            assert t != null;
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return arrayList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        assert typeAr != null && typeAr.length >= 1;
        for (int i = 0; i < typeAr.length; i++) {
            arrayList.add(new TDItem(typeAr[i], fieldAr == null ? null : fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return arrayList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields()) {
            throw new NoSuchElementException("requesting index " + i);
        }

        return arrayList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields()) {
            throw new NoSuchElementException("requesting index " + i);
        }
        return arrayList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < arrayList.size(); i++) {
            TDItem item = arrayList.get(i);
            if (name == null) {
                if (item.fieldName == null) {
                    return i;
                }
            } else {
                if (name.equals(item.fieldName)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException("requesting index name:" + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int result = 0;
        for (TDItem item : arrayList) {
            result += item.fieldType.getLen();
        }
        return result;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int len = td1.numFields() + td2.numFields();
        Type[] t = new Type[len];
        String[] f = new String[len];
        for (int i = 0; i < td1.numFields(); i++) {
            t[i] = td1.getFieldType(i);
            f[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); i++) {
            t[i + td1.numFields()] = td2.getFieldType(i);
            f[i + td1.numFields()] = td2.getFieldName(i);
        }

        return new TupleDesc(t, f);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if (numFields() != other.numFields()) {
            return false;
        }
        Iterator<TDItem> ii = iterator();
        Iterator<TDItem> jj = other.iterator();
        while (ii.hasNext() && jj.hasNext()) {
            TDItem i = ii.next();
            TDItem j = jj.next();
            if (i.fieldType != j.fieldType) {
                return false;
            }
        }
        assert ii.hasNext() == false && jj.hasNext() == false;
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
//        throw new UnsupportedOperationException("unimplemented");
        String s = toString();
        return s.hashCode();
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
        StringBuffer buffer = new StringBuffer();
        Iterator<TDItem> iterator = iterator();
        while (iterator.hasNext()) {
            TDItem item = iterator.next();
            buffer.append(item.fieldType).append("(").append(item.fieldName).append(")");
            buffer.append(",");
        }
        if (buffer.length() > 1) {
            buffer.deleteCharAt(buffer.length() - 1);
        }
        return buffer.toString();
    }
}