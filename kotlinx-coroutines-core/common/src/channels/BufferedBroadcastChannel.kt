/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.channels

import kotlinx.atomicfu.locks.*
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.Channel.Factory.RENDEZVOUS
import kotlinx.coroutines.channels.ChannelResult.Companion.success
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.internal.ReentrantLock
import kotlinx.coroutines.internal.withLock
import kotlinx.coroutines.selects.*
import kotlin.native.concurrent.*

/**
 * Broadcast channel with array buffer of a fixed [capacity].
 * Sender suspends only when buffer is full due to one of the receives being slow to consume and
 * receiver suspends only when buffer is empty.
 *
 * **Note**, that elements that are sent to this channel while there are no
 * [openSubscription] subscribers are immediately lost.
 *
 * This channel is created by `BroadcastChannel(capacity)` factory function invocation.
 */
internal open class BufferedBroadcastChannel<E>(
    /**
     * Buffer capacity.
     */
    val capacity: Int
) : BufferedChannel<E>(capacity = RENDEZVOUS, onUndeliveredElement = null), BroadcastChannel<E> {
    init {
        require(capacity >= 1 || capacity == CONFLATED) { "ArrayBroadcastChannel capacity must be at least 1, but $capacity was specified" }
    }

    private val lock = ReentrantLock()
    private val subscribers: MutableList<BufferedChannel<E>> = mutableListOf()

    private var lastConflatedElement: Any? = NO_ELEMENT // NO_ELEMENT or E

    /**
     * The most recently sent element to this channel.
     *
     * Access to this property throws [IllegalStateException] when this class is constructed without
     * initial value and no value was sent yet or if it was [closed][close] without a cause.
     * It throws the original [close][SendChannel.close] cause exception if the channel has _failed_.
     */
    @Suppress("UNCHECKED_CAST")
    public val value: E get() = lock.withLock {
        if (isClosedForReceive) {
            throw closeCause2 ?: IllegalStateException("This broadcast channel is closed")
        }
        lastConflatedElement.let {
            if (it !== NO_ELEMENT) it as E
            else error("No value")
        }
    }

    /**
     * The most recently sent element to this channel or `null` when this class is constructed without
     * initial value and no value was sent yet or if it was [closed][close].
     */
    public val valueOrNull: E? get() = lock.withLock {
        if (isClosedForReceive) null
        else if (lastConflatedElement === NO_ELEMENT) null
        else lastConflatedElement as E
    }

    public override fun openSubscription(): ReceiveChannel<E> = lock.withLock {
        val s = if (capacity == CONFLATED) SubscriberConflated() else SubscriberBuffered()
        if (isClosedForSend && lastConflatedElement === NO_ELEMENT) {
            s.close(closeCause2)
            return@withLock s
        }
        if (lastConflatedElement !== NO_ELEMENT) {
            s.trySend(value)
        }
        subscribers += s
        s
    }

    private fun removeSubscriberUsync(s: ReceiveChannel<E>) {
        subscribers.remove(s)
    }

    override suspend fun send(element: E) {
        val subs = lock.withLock {
            if (isClosedForSend) throw sendException(trySend(element).exceptionOrNull())
            if (capacity == CONFLATED)
                lastConflatedElement = element
            ArrayList(subscribers)
        }
        subs.forEach { it.sendBroadcast(element) }
    }

    override fun trySend(element: E): ChannelResult<Unit> = lock.withLock {
        if (isClosedForSend) return super.trySend(element)
        val success = subscribers.none { it.shouldSendSuspend() }
        if (!success) return ChannelResult.failure()
        subscribers.forEach { it.trySend(element) }
        if (capacity == CONFLATED)
            lastConflatedElement = element
        return success(Unit)
    }

    override fun registerSelectForSend(select: SelectInstance<*>, element: Any?) = lock.withLock {
        error("TODO")
    }

    override fun close(cause: Throwable?): Boolean = lock.withLock {
        subscribers.forEach { it.close(cause) }
        subscribers.clear()
        return super.close(cause)
    }

    override fun cancelImpl(cause: Throwable?): Boolean = lock.withLock {
        super.cancelImpl(cause).also {
            subscribers.forEach {
                it as Subscriber<E>
                it.cancelImplWithoutRemovingSubscriber(cause)
            }
            subscribers.clear()
            lastConflatedElement = NO_ELEMENT
        }
    }

    override val isClosedForSend: Boolean
        get() = lock.withLock { super.isClosedForSend }

    private interface Subscriber<E> : ReceiveChannel<E> {
        fun cancelImplWithoutRemovingSubscriber(cause: Throwable?)
    }

    private inner class SubscriberBuffered : BufferedChannel<E>(capacity = capacity), Subscriber<E> {
        override fun tryReceive(): ChannelResult<E> {
            lock.lock()
            return super.tryReceive()
        }

        override suspend fun receive(): E {
            lock.lock()
            return super.receive()
        }

        override suspend fun receiveCatching(): ChannelResult<E> {
            lock.lock()
            return super.receiveCatching()
        }

        override fun registerSelectForReceive(select: SelectInstance<*>, ignoredParam: Any?) {
            lock.lock()
            super.registerSelectForReceive(select, ignoredParam)
        }

        override fun iterator() = SubscriberIterator()

        override fun onReceiveSynchronizationCompletion() {
            super.onReceiveSynchronizationCompletion()
            lock.unlock()
        }

        override val isClosedForReceive: Boolean
            get() = lock.withLock { super.isClosedForReceive }

        override val isEmpty: Boolean
            get() = lock.withLock { super.isEmpty }

        public override fun cancelImpl(cause: Throwable?): Boolean = lock.withLock {
            removeSubscriberUsync(this@SubscriberBuffered )
            super.cancelImpl(cause)
        }

        override fun cancelImplWithoutRemovingSubscriber(cause: Throwable?) {
            super.cancelImpl(cause)
        }

        private inner class SubscriberIterator : BufferedChannelIterator() {
            override suspend fun hasNext(): Boolean {
                lock.lock()
                return super.hasNext()
            }
        }
    }

    private inner class SubscriberConflated
        : ConflatedBufferedChannel<E>(capacity = 1, onBufferOverflow = BufferOverflow.DROP_OLDEST), Subscriber<E>
    {
        override fun tryReceive(): ChannelResult<E> {
            lock.lock()
            return super.tryReceive()
        }

        override suspend fun receive(): E {
            lock.lock()
            return super.receive()
        }

        override suspend fun receiveCatching(): ChannelResult<E> {
            lock.lock()
            return super.receiveCatching()
        }

        override fun registerSelectForReceive(select: SelectInstance<*>, ignoredParam: Any?) {
            lock.lock()
            super.registerSelectForReceive(select, ignoredParam)
        }

        override fun iterator() = SubscriberIterator()

        override fun onReceiveSynchronizationCompletion() {
            super.onReceiveSynchronizationCompletion()
            lock.unlock()
        }

        override val isClosedForReceive: Boolean
            get() = lock.withLock { super.isClosedForReceive }

        override val isEmpty: Boolean
            get() = lock.withLock { super.isEmpty }

        public override fun cancelImpl(cause: Throwable?): Boolean = lock.withLock {
            removeSubscriberUsync(this@SubscriberConflated )
            super.cancelImpl(cause)
        }

        override fun cancelImplWithoutRemovingSubscriber(cause: Throwable?) {
            super.cancelImpl(cause)
        }

        private inner class SubscriberIterator : ConflatedChannelIterator() {
            override suspend fun hasNext(): Boolean {
                lock.lock()
                return super.hasNext()
            }
        }
    }

    override fun toString() =
        "conflatedElement=$lastConflatedElement," +
        "broadcast=${super.toString()}," +
        "subscribers=${subscribers.joinToString(separator = ";", prefix = "<", postfix = ">")}"
}

@SharedImmutable
private val NO_ELEMENT = Symbol("NO_ELEMENT")
