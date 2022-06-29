package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op operator;

    private Tuple[] tuples;

    private int cursor;

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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.operator = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // MIN, MAX, SUM, AVG, COUNT,
        if (operator.equals(Op.MIN)) {

        } else if(operator.equals(Op.MAX)) {

        } else if(operator.equals(Op.SUM)) {

        } else if(operator.equals(Op.AVG)) {
            tup.getField(gbfield);
            for(Iterator<Field> fieldIterator = tup.fields(); fieldIterator.hasNext(); ) {
                Field field = fieldIterator.next();
            }
            tup.setField(gbfield, );
        } else if(operator.equals(Op.COUNT)) {
            tup.setField(gbfield, tuples[cursor]);
            tuples[cursor] = tup;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private int cursor;

            private int len;

            @Override
            public void open() throws DbException, TransactionAbortedException {

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {

            }

            @Override
            public TupleDesc getTupleDesc() {
                return null;
            }

            @Override
            public void close() {

            }
        }
    }

}
