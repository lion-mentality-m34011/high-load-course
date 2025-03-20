package ru.quipy.common.utils

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Condition
import java.util.LinkedList

class BufferedSemaphore(maxPermits: Int, private val maxQueueSize: Int) {
    private val lock = ReentrantLock()
    private val condition: Condition = lock.newCondition()
    private var permits = maxPermits
    private val waitQueue = LinkedList<Thread>()

    fun acquire(): Boolean {
        lock.lock()
        try {
            if (waitQueue.size >= maxQueueSize) {
                return false
            }
            val currentThread = Thread.currentThread()
            waitQueue.add(currentThread)

            while (permits == 0 || waitQueue.peek() != currentThread) {
                condition.await()
            }

            waitQueue.poll()
            permits--
            return true
        } finally {
            lock.unlock()
        }
    }

    fun release() {
        lock.lock()
        try {
            permits++
            condition.signalAll()
        } finally {
            lock.unlock()
        }
    }
}