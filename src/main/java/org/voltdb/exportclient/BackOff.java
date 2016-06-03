package org.voltdb.exportclient;

import java.util.concurrent.ThreadLocalRandom;

abstract class BackOff {
    int base;
    int cap;

    public BackOff(int base, int cap) {
        this.base = base;
        this.cap = cap;
    }

    public int expo(int n) {
        return Math.min(cap, base * (1<<n));
    }

    abstract public int backoff(int n);
}

class ExpoBackOffFullJitter extends BackOff {

    public ExpoBackOffFullJitter(int base, int cap) {
        super(base, cap);
    }

    @Override
    public int backoff(int n) {
        return ThreadLocalRandom.current().nextInt(expo(n));
    }
}

class ExpoBackOffEqualJitter extends BackOff {

    public ExpoBackOffEqualJitter(int base, int cap) {
        super(base, cap);
    }

    @Override
    public int backoff(int n) {
        int v = expo(n) / 2 ;
        return ThreadLocalRandom.current().nextInt(v, 3 * v);
    }
}

class ExpoBackOffDecor extends BackOff {
    int sleep;
    public ExpoBackOffDecor(int base, int cap) {
        super(base, cap);
        sleep = base;
    }

    @Override
    public int backoff(int n) {
        sleep = Math.min(cap, ThreadLocalRandom.current().nextInt(sleep * (1+n) + base));
        return sleep;
    }
}