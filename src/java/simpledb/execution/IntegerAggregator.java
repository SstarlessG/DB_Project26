package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> aggregateMap = new HashMap<>();
    private Map<Field, Integer> countMap = new HashMap<>();

    private static final long serialVersionUID = 1L;

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
        this.what = what;
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
        Field groupkey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        int value = ((IntField) tup.getField(afield)).getValue();

        switch(what){
            case MIN:
                aggregateMap.put(groupkey, aggregateMap.containsKey(groupkey) ? Math.min(aggregateMap.get(groupkey), value): value);
                break;

            case MAX:
                aggregateMap.put(groupkey, aggregateMap.containsKey(groupkey) ? Math.max(aggregateMap.get(groupkey), value): value);
                break;

            case SUM:
                aggregateMap.put(groupkey, aggregateMap.getOrDefault(groupkey, 0) + value);
                break;
            
            case COUNT:
                aggregateMap.put(groupkey, aggregateMap.getOrDefault(groupkey, 0) + 1);
                break;
            
            case AVG:
                aggregateMap.put(groupkey, aggregateMap.getOrDefault(groupkey, 0) + value);
                countMap.put(groupkey, countMap.getOrDefault(groupkey, 0) + 1);
                break;

            default:
                throw new UnsupportedOperationException();
            
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
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;

        if (gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        for (Field groupkey: aggregateMap.keySet()){

            Tuple tuple = new Tuple(td);
            int result = aggregateMap.get(groupkey);

            if (what == Op.AVG){
                result = result / countMap.get(groupkey);
            }

            if (gbfield == NO_GROUPING){
                tuple.setField(0, new IntField(result));
            } else {
                tuple.setField(0, groupkey);
                tuple.setField(1, new IntField(result));
            }

            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }
}
