package com.flytxt.miscellaneous.locking;

/**
 * The DistributedLock class
 *
 * @author sivasyam
 *
 */
public interface DistributedLock {

    public void accquireLock() throws Exception;

    public void releaseLock() throws Exception;

    public boolean isLocked() throws Exception;
}