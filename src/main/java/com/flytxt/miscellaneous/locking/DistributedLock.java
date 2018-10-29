package com.flytxt.miscellaneous.locking;

/**
 * The DistributedLock class
 *
 * @author sivasyam
 *
 */
public interface DistributedLock {

    public void accquire() throws Exception;

    public void release() throws Exception;

    public boolean isLocked() throws Exception;
}