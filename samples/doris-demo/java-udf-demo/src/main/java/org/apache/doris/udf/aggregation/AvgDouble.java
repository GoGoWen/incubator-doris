package org.apache.doris.udf.aggregation;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.logging.Logger;

/**
 * avg_double
 * select t.name, avg_double(t.age) as avg_age from (select '2' as name, 2 as age union all select '2' as name, 1 as age ) t group by t.name;
 */
public class AvgDouble {

    Logger log = Logger.getLogger("AvgDouble");

    //Need an inner class to store data
    public static class State {
        /*some variables if you need */
        public Double sum = 0.0;
        public Integer count = 0;

        public byte[] getVal() {
            byte s = sum.byteValue();
            byte c = count.byteValue();
            return new byte[]{s, c};
        }
    }

    public State create() {
        /* here could do some init work if needed */
        return new State();
    }

    public void destroy(State state) {
        /* here could do some destroy work if needed */
    }

    public void reset(State state) {
        /*if you want this udaf function can work with window function.*/
        /*Must impl this, it will be reset to init state after calculate every window frame*/
        state.sum = 0.0;
        state.count = 0;
    }

    //first argument is State, then other types your input
    public void add(State state, Double val) throws Exception {
        /* here doing update work when input data*/
        if (val != null) {
            state.sum += val;
            state.count += 1;
        }
    }

    public void serialize(State state, DataOutputStream out) {
        /* serialize some data into buffer */
        try {
            out.write(state.getVal());
        } catch (Exception e) {
            /* Do not throw exceptions */
            log.info(e.getMessage());
        }
    }

    public void deserialize(State state, DataInputStream in) {
        /* deserialize get data from buffer before you put */
        try {
            byte[] buffer = new byte[2];
            in.readFully(buffer);
            state.sum = (double) buffer[0];
            state.count = (int) buffer[1];
        } catch (Exception e) {
            /* Do not throw exceptions */
            log.info(e.getMessage());
        }
    }

    public void merge(State state, State rhs) throws Exception {
        /* merge data from state */
        state.sum += rhs.sum;
        state.count += rhs.count;
    }

    //return Type you defined
    public double getValue(State state) throws Exception {
        /* return finally result */
        if (state.count == 0) {
            return state.sum;
        } else {
            return state.sum / state.count;
        }
    }
}
