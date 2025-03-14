package ru.quipy.common.utils

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Condition
import java.util.LinkedList

class FuckingSemaphore(private val maxPermits: Int, private val maxQueueSize: Int) {
    private val lock = ReentrantLock()
    private val condition: Condition = lock.newCondition()
    private var permits = maxPermits
    private val waitQueue = LinkedList<Thread>()

    fun acquire(): Boolean {
        lock.lock()
        try {
            if (waitQueue.size >= maxQueueSize) {
                println("❌ Очередь забита нахуй! Поток ${Thread.currentThread().id} идёт в жопу!")
                return false
            }
            val currentThread = Thread.currentThread()
            waitQueue.add(currentThread)

            while (permits == 0 || waitQueue.peek() != currentThread) {
                condition.await()
            }

            waitQueue.poll()
            permits--
            println("🔥 Поток ${currentThread.id} зашёл в ебучий семафор!")
            return true
        } finally {
            lock.unlock()
        }
    }

    fun release() {
        lock.lock()
        try {
            permits++
            println("💨 Поток ${Thread.currentThread().id} вышел из ёбаного семафора!")
            condition.signalAll()
        } finally {
            lock.unlock()
        }
    }
}
