package org.voltdb.exportclient;

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
            return new ExpoBackOffDecor(backOffBase, backOffCap);
        }
    }
}