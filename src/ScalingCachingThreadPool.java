import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.isNull;

/**
 * Immediately add workers to maximum when submitting new tasks
 */
public class ScalingCachingThreadPool extends ThreadPoolExecutor {

    /**
     * Try to enqueue task if no available workers
     */
    private static final RejectedExecutionHandler rejectionHandler = (task, executor) -> {
        try {
            if (executor.getQueue().remainingCapacity() <= 0) {
                throw new RejectedExecutionException(format("Task %s rejected from %s", task, executor));
            }
            executor.getQueue().put(task);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    };

    public ScalingCachingThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit
    ) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, true);
    }

    public ScalingCachingThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            boolean prestartCoreThreads
    ) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, prestartCoreThreads, new LinkedBlockingQueue<>());
    }

    public ScalingCachingThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue
    ) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, true, workQueue);
    }

    public ScalingCachingThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            boolean prestartCoreThreads,
            BlockingQueue<Runnable> workQueue
    ) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, prestartCoreThreads, workQueue, null);
    }

    public ScalingCachingThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            boolean prestartCoreThreads,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler
    ) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, prestartCoreThreads, workQueue, handler, Executors.defaultThreadFactory());
    }

    public ScalingCachingThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            boolean prestartCoreThreads,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler,
            ThreadFactory threadFactory
    ) {
        super(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                new ScalingBlockingQueueFacade<>(workQueue),
                threadFactory,
                isNull(handler) ? rejectionHandler : new CombiningRejectionHandler(rejectionHandler, handler)
        );
        if (prestartCoreThreads) {
            prestartCoreThread();
        }
    }

    /**
     * Facade to user specified blocking queue with only offer method rewritten
     */
    private static class ScalingBlockingQueueFacade<E> extends AbstractQueue<E> implements BlockingQueue<E>, java.io.Serializable {

        private final BlockingQueue<E> delegate;

        public ScalingBlockingQueueFacade(BlockingQueue<E> delegate) {
            this.delegate = delegate;
        }

        /**
         * Always ask thread pool to create new worker until have remaining capacity
         */
        @Override
        public boolean offer(E e) {
            if (size() == 0) {
                return delegate.offer(e);
            } else {
                return false;
            }
        }

        //----------------- Other methods just call delegate

        @Override
        public Iterator<E> iterator() {
            return delegate.iterator();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public void put(E e) throws InterruptedException {
            delegate.put(e);
        }

        @Override
        public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.offer(e, timeout, unit);
        }


        @Override
        public E take() throws InterruptedException {
            return delegate.take();
        }

        @Override
        public E poll(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.poll(timeout, unit);
        }

        @Override
        public int remainingCapacity() {
            return delegate.remainingCapacity();
        }

        @Override
        public int drainTo(Collection<? super E> c) {
            return delegate.drainTo(c);
        }

        @Override
        public int drainTo(Collection<? super E> c, int maxElements) {
            return delegate.drainTo(c, maxElements);
        }

        @Override
        public E poll() {
            return delegate.poll();
        }

        @Override
        public E peek() {
            return delegate.peek();
        }

        @Override
        public boolean add(E e) {
            return delegate.add(e);
        }

        @Override
        public E remove() {
            return delegate.remove();
        }

        @Override
        public E element() {
            return delegate.element();
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            return delegate.addAll(c);
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return delegate.contains(o);
        }


        @Override
        public Object[] toArray() {
            return delegate.toArray();
        }


        @Override
        public <T> T[] toArray(T[] a) {
            return delegate.toArray(a);
        }

        @Override
        public boolean remove(Object o) {
            return delegate.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return delegate.containsAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return delegate.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return delegate.removeAll(c);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public <T> T[] toArray(IntFunction<T[]> generator) {
            return delegate.toArray(generator);
        }

        @Override
        public boolean removeIf(Predicate<? super E> filter) {
            return delegate.removeIf(filter);
        }

        @Override
        public Spliterator<E> spliterator() {
            return delegate.spliterator();
        }

        @Override
        public Stream<E> stream() {
            return delegate.stream();
        }

        @Override
        public Stream<E> parallelStream() {
            return delegate.parallelStream();
        }

        @Override
        public void forEach(Consumer<? super E> action) {
            delegate.forEach(action);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return delegate.equals(obj);
        }
    }

    /**
     * Utility handler class to combine with user specified handlers
     */
    private static class CombiningRejectionHandler implements RejectedExecutionHandler {

        private final RejectedExecutionHandler[] handlers;

        public CombiningRejectionHandler(RejectedExecutionHandler... handlers) {
            this.handlers = handlers;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            for (RejectedExecutionHandler handler : handlers) {
                handler.rejectedExecution(r, executor);
            }
        }
    }
}
