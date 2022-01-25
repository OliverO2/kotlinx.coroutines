/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.channels

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.selects.*
import kotlin.native.concurrent.*

/**
 * Broadcasts the most recently sent element (aka [value]) to all [openSubscription] subscribers.
 *
 * Back-to-send sent elements are _conflated_ -- only the most recently sent value is received,
 * while previously sent elements **are lost**.
 * Every subscriber immediately receives the most recently sent element.
 * Sender to this broadcast channel never suspends and [trySend] always succeeds.
 *
 * A secondary constructor can be used to create an instance of this class that already holds a value.
 * This channel is also created by `BroadcastChannel(Channel.CONFLATED)` factory function invocation.
 *
 * In this implementation, [opening][openSubscription] and [closing][ReceiveChannel.cancel] subscription
 * takes linear time in the number of subscribers.
 *
 * **Note: This API is obsolete since 1.5.0.** It will be deprecated with warning in 1.6.0
 * and with error in 1.7.0. It is replaced with [StateFlow][kotlinx.coroutines.flow.StateFlow].
 */
@ObsoleteCoroutinesApi
internal class ConflatedBroadcastChannel<E>() :
    BufferedBroadcastChannel<E>(capacity = Channel.CONFLATED),
    BroadcastChannel<E>
{
    /**
     * Creates an instance of this class that already holds a value.
     *
     * It is as a shortcut to creating an instance with a default constructor and
     * immediately sending an element: `ConflatedBroadcastChannel().apply { offer(value) }`.
     */
    public constructor(value: E) : this() {
        trySend(value)
    }
}