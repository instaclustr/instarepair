package com.instaclustr.instarepair;

import java.util.*;

/**
 * Round Robin elements from given set of iterators.
 */
public class RoundRobinIterator<T> implements Iterator<T> {
    private Deque<Iterator<T>> queue;

    /**
     * The next value to return or null if not yet set.
     */
    private T nextValue;


    public RoundRobinIterator(Collection<Iterable<T>> elements) {
        this.queue = new ArrayDeque<Iterator<T>>(elements.size());
        for (Iterable<T> iterator : elements) {
            queue.add(iterator.iterator());
        }
    }

    @Override
    public boolean hasNext() {
        // Next value already found, do nothing.
        if (nextValue != null) {
            return true;
        }

        while (true) {
            // No elements left. Finished.
            if (queue.isEmpty()) {
                return false;
            }

            // Get the next iterator.
            Iterator<T> iterator = queue.removeFirst();
            if (iterator.hasNext()) {
                nextValue = iterator.next();
                // Re-add to queue if it has more elements.
                if (iterator.hasNext()) {
                    queue.addLast(iterator);
                }
                return true;
            }
        }
    }

    @Override
    public T next() {
        if (!hasNext())
            throw new NoSuchElementException();
        T n = nextValue;
        nextValue = null;
        return n;
    }
}
