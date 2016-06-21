/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.lang.reflect.Constructor;

import com.google_voltpatches.common.base.Throwables;

public class BackOffFactory {
    public static BackOff getBackOff(String backOffType, int backOffBase, int backOffCap) {
        switch (backOffType) {
        case "full":
            return new ExpoBackOffFullJitter(backOffBase, backOffCap);
        case "equal":
            return new ExpoBackOffEqualJitter(backOffBase, backOffCap);
        case "decor":
            return new ExpoBackOffDecor(backOffBase, backOffCap);
        default:
            try {
                Constructor<?> c = Class.forName(backOffType).getConstructor(Integer.TYPE, Integer.TYPE);
                BackOff backoff = (BackOff) c.newInstance(backOffBase, backOffCap);
                return backoff;
            } catch(Throwable t) {
                Throwables.propagate(t);
            }
            return new ExpoBackOffDecor(backOffBase, backOffCap);
        }
    }
}