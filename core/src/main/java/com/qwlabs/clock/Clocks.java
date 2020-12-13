package com.qwlabs.clock;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

@NotThreadSafe
public class Clocks implements Iterable<Clocks.Entry> {
    private int[] clockArray = null;

    public Clocks() {
    }

    public Clocks(int size) {
        this.clockArray = new int[size * 2];
        for (int index = 0; index < this.clockArray.length; index += 2) {
            this.clockArray[index] = index;
        }
    }

    public Clocks(Clocks clocks) {
        if (clocks.clockArray == null) {
            this.clockArray = null;
        } else {
            this.clockArray = Arrays.copyOf(clocks.clockArray, clocks.clockArray.length);
        }
    }

    public int getClock(int nodeIndex) {
        if (this.clockArray == null) {
            return 0;
        }
        for (int index = 0; index < this.clockArray.length; index += 2) {
            if (this.clockArray[index] == nodeIndex) {
                return this.clockArray[index + 1];
            }
        }
        return 0;
    }

    public int set(int nodeIndex, int clock) {
        if (clock == 0) {
            return remove(nodeIndex);
        }
        if (this.clockArray == null) {
            this.clockArray = new int[2];
            this.clockArray[0] = nodeIndex;
            this.clockArray[1] = clock;
            return 0;
        }
        for (int index = 0; index < this.clockArray.length; index += 2) {
            if (this.clockArray[index] == nodeIndex) {
                int oldClock = this.clockArray[index + 1];
                this.clockArray[index + 1] = clock;
                return oldClock;
            }
        }
        lengthen();
        this.clockArray[this.clockArray.length - 2] = nodeIndex;
        this.clockArray[this.clockArray.length - 1] = clock;
        return 0;
    }

    public int tick(int nodeIndex) {
        if (this.clockArray == null) {
            this.clockArray = new int[2];
            this.clockArray[0] = nodeIndex;
            this.clockArray[1] = 1;
            return 0;
        }
        for (int index = 0; index < this.clockArray.length; index += 2) {
            if (this.clockArray[index] == nodeIndex) {
                int oldClock = this.clockArray[index + 1];
                this.clockArray[index + 1] = this.clockArray[index + 1] + 1;
                return oldClock;
            }
        }
        set(nodeIndex, 1);
        return 0;
    }

    public void set(int nodeIndex, int clock, int index) {
        int clockArrayIndex = index * 2;
        this.clockArray[clockArrayIndex] = nodeIndex;
        this.clockArray[clockArrayIndex + 1] = clock;
    }

    public int remove(int nodeIndex) {
        if (this.clockArray == null) {
            return 0;
        }
        for (int index = 0; index < this.clockArray.length; index += 2) {
            if (this.clockArray[index] == nodeIndex) {
                this.clockArray[index] = this.clockArray[this.clockArray.length - 2];
                int clock = this.clockArray[index + 1];
                this.clockArray[index + 1] = this.clockArray[this.clockArray.length - 1];
                shorten();
                return clock;
            }
        }
        return 0;
    }

    public void merge(Clocks other) {
        int[] otherClockArray = other.clockArray;
        if (otherClockArray == null) {
            return;
        }
        for (int index = 0; index < otherClockArray.length; index += 2) {
            int nodeIndex = otherClockArray[index];
            int clock = getClock(nodeIndex);
            int otherClock = otherClockArray[index + 1];
            if (otherClock > clock) {
                set(nodeIndex, otherClock);
            }
        }
    }

    public int size() {
        return this.clockArray == null ? 0 : (this.clockArray.length / 2);
    }

    public boolean isEmpty() {
        return this.clockArray == null ? true : (this.clockArray.length == 0);
    }

    private void shorten() {
        if (this.clockArray == null || this.clockArray.length == 0) {
            return;
        }
        int[] arrayOfInt = new int[this.clockArray.length - 2];
        System.arraycopy(this.clockArray, 0, arrayOfInt, 0, arrayOfInt.length);
        this.clockArray = arrayOfInt;
    }

    private void lengthen() {
        int[] arrayOfInt = new int[this.clockArray.length + 2];
        System.arraycopy(this.clockArray, 0, arrayOfInt, 0, this.clockArray.length);
        this.clockArray = arrayOfInt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Clocks clocks = (Clocks) o;
        return Arrays.equals(clockArray, clocks.clockArray);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(clockArray);
    }

    public Iterator<Entry> iterator() {
        return new VectorIterator(this);
    }

    @Immutable
    public static class Entry {
        private final int nodeIndex;

        private final int clock;

        private Entry(int nodeIndex, int clock) {
            this.nodeIndex = nodeIndex;
            this.clock = clock;
        }

        public int getNodeIndex() {
            return nodeIndex;
        }

        public int getClock() {
            return clock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry entry = (Entry) o;
            return nodeIndex == entry.nodeIndex && clock == entry.clock;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeIndex, clock);
        }
    }


    private static class VectorIterator implements Iterator<Entry> {
        private final Clocks clocks;

        private int index = 0;

        private VectorIterator(Clocks clocks) {
            this.clocks = clocks;
        }

        public boolean hasNext() {
            return (this.clocks.clockArray == null) ? false : ((this.index < this.clocks.clockArray.length));
        }

        public Entry next() {
            if (!hasNext()) {
                return null;
            }
            Entry entry = new Entry(this.clocks.clockArray[this.index], this.clocks.clockArray[this.index + 1]);
            this.index += 2;
            return entry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
