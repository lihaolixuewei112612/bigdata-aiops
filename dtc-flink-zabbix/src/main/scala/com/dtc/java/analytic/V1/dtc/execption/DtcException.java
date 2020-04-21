package com.dtc.java.analytic.V1.dtc.execption;

/**
 * Created on 2019-08-16
 *
 * @author :hao.li
 */
public class DtcException extends RuntimeException {

    private static final long serialVersionUID = 3779180728566653711L;

    public DtcException() {
        super("DtcException!");
    }

    public DtcException(String msg) {
        super(msg);
    }

    public DtcException(String msg, Throwable th) {
        super(msg, th);
    }

    public DtcException(Throwable th) {
        super(th);
    }
}
