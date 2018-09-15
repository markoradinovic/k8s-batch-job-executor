package org.cube.k8s.k8sbatchjobexecutor;

@SuppressWarnings("WeakerAccess")
public class Utils {

    public static int nullSafeInt(Integer integer) {
        return integer != null ? integer : -1;
    }
}
