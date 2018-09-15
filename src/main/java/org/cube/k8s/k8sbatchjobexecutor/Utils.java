package org.cube.k8s.k8sbatchjobexecutor;

public class Utils {

    public static int nullSafeInt(Integer integer) {
        return integer != null ? integer.intValue() : -1;
    }
}
