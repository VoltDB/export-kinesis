/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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