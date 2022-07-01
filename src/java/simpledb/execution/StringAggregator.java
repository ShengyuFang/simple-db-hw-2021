package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
//TODO NO_GROUPING
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op operator;

    private Map<Field, Tuple> aggregateFieldToTuple;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.operator = what;
        this.aggregateFieldToTuple = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = tup.getField(gbfield);
        Tuple tuple = aggregateFieldToTuple.computeIfAbsent(groupField, key -> {
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{tup.getTupleDesc().getFieldName(gbfield), operator.name()}
            );
            return new Tuple(tupleDesc);
        });
        tuple.setField(0, tup.getField(gbfield));
        if (operator.equals(Op.COUNT)) {
            IntField field = (IntField)tuple.getField(1);
            if(field == null) {
                tuple.setField(1, new IntField(1));
            } else {
                tuple.setField(1, new IntField(field.getValue() + 1));
            }
        } else if (operator.equals(Op.SUM)) {
            IntField field = (IntField)tuple.getField(1);
            int value = ((IntField)tup.getField(afield)).getValue();
            if(field != null) {
                value += field.getValue();
            }
            tuple.setField(1, new IntField(value));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            private boolean open;

            private Iterator<Tuple> tuples;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                tuples = aggregateFieldToTuple.values().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return open && tuples.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return open ? tuples.next() : null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tuples.next().getTupleDesc();
            }

            @Override
            public void close() {
                open = false;
            }
        };
    }

}
