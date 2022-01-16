/*
 * Copyright 2016-2018 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("DEPRECATION")

package kotlinx.coroutines.channels

import kotlinx.coroutines.*
import kotlin.coroutines.*
import kotlin.math.*
import kotlin.test.*

class ChannelsTest: TestBase() {
    private val testList = listOf(1, 2, 3)

    @Test
    fun testIterableAsReceiveChannel() = runTest {
        assertEquals(testList, testList.asReceiveChannel().toList())
    }

    @Test
    fun testCloseWithMultipleWaiters() = runTest {
        // 1. Coroutine #1 suspends on `receive()`
        // 2. Coroutine #2 suspends on `receive()`
        // 3. The channel closes
        // 4. Coroutine #2 fails with exception (last suspends => first fails)
        // 5. Coroutine #1 fails with exception
        val channel = Channel<Int>()
        launch {
            try {
                expect(2)
                channel.receive()
                expectUnreached()
            } catch (e: ClosedReceiveChannelException) {
                expect(6)
            }
        }

        launch {
            try {
                expect(3)
                channel.receive()
                expectUnreached()
            } catch (e: ClosedReceiveChannelException) {
                expect(5)
            }
        }

        expect(1)
        yield()
        expect(4)
        channel.close()
        yield()
        finish(7)
    }

    @Test
    fun testEmptyList() = runTest {
        assertTrue(emptyList<Nothing>().asReceiveChannel().toList().isEmpty())
    }

    @Test
    fun testToList() = runTest {
        assertEquals(testList, testList.asReceiveChannel().toList())

    }

    private fun <E> Iterable<E>.asReceiveChannel(context: CoroutineContext = Dispatchers.Unconfined): ReceiveChannel<E> =
        GlobalScope.produce(context) {
            for (element in this@asReceiveChannel)
                send(element)
        }
}
