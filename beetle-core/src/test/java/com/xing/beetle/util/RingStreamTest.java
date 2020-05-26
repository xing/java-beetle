package com.xing.beetle.util;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

public class RingStreamTest {

    @Test
    public void testEmptyStream() {
        RingStream<String> s = new RingStream<>();
        assertEquals(0, s.size());
        assertFalse(s.hasNext());
        assertThrows(NoSuchElementException.class, s::next);
    }


    @Test
    public void testStreamRollover(){
        RingStream<String> s = new RingStream<>("foo", "bar", "baz");
        assertEquals("foo", s.next());
        assertEquals("bar", s.next());
        assertEquals("baz", s.next());
        assertEquals(3, s.streamAll().count());
        assertTrue(s.hasNext());
        assertEquals("foo", s.next());
    }

    @Test
    public void testLimitedStream(){
        RingStream<String> s = new RingStream<>("foo", "bar", "baz");
        assertTrue(s.streamLimited(2).noneMatch("baz"::equals));
        assertTrue(s.streamLimited(2).noneMatch("bar"::equals));
        assertTrue(s.streamLimited(2).noneMatch("foo"::equals));
    }
}
