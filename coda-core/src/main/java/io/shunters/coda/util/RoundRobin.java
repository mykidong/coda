package io.shunters.coda.util;

import java.util.Iterator;
import java.util.List;

/**
 * Created by mykidong on 2017-09-28.
 */
public class RoundRobin {

    private Iterator<Robin> it;
    private List<Robin> list;

    public RoundRobin(List<Robin> list) {
        this.list = list;
        it = list.iterator();
    }

    public int next() {
        // if we get to the end, start again
        if (!it.hasNext()) {
            it = list.iterator();
        }
        Robin robin = it.next();

        return robin.call();
    }

    public static class Robin {
        private int i;

        public Robin(int i) {
            this.i = i;
        }

        public int call() {
            return i;
        }
    }
}
