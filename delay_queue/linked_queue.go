package delay_queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// v1为有锁版本实现 test里面有基准测试
// 并发安全队列
type ConcurrentLinkedQueue[T any] struct {
	// *node[T]
	head unsafe.Pointer
	// *node[T]
	tail unsafe.Pointer
	lock *sync.Mutex
	miss int64
}

func NewConcurrentLinkedQueue[T any]() *ConcurrentLinkedQueue[T] {
	head := &node[T]{}
	ptr := unsafe.Pointer(head)
	var mutex sync.Mutex
	return &ConcurrentLinkedQueue[T]{
		head: ptr,
		tail: ptr,
		lock: &mutex,
		miss: 0,
	}
}

func (c *ConcurrentLinkedQueue[T]) Enqueue(t T) error {
	var (
		newNode *node[T]
		newPtr  unsafe.Pointer
	)

	newNode = &node[T]{val: t, wait: 1}
	newPtr = unsafe.Pointer(newNode)

	for {

		tailPtr := atomic.LoadPointer(&c.tail)
		tail := (*node[T])(tailPtr)
		tailNext := atomic.LoadPointer(&tail.next)
		if tailNext != nil {
			// 已经被人修改了，不需要修复，因为预期中修改的那个人会把 c.tail 指过去
			//atomic.AddInt64(&newNode.wait, 1)
			newNode.wait++
			continue
		}
		if atomic.CompareAndSwapPointer(&tail.next, tailNext, newPtr) {
			// 如果失败也不用担心，说明有人抢先一步了
			atomic.CompareAndSwapPointer(&c.tail, tailPtr, newPtr)
			return nil
		}
		atomic.AddInt64(&c.miss, 1)
		time.Sleep(10000 * time.Nanosecond)
	}
	return nil
}
func (c *ConcurrentLinkedQueue[T]) EnqueueV1(t T) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	newNode := &node[T]{val: t}
	newPtr := unsafe.Pointer(newNode)
	fmt.Println((*node[T])(newPtr).val)
	tailPtr := atomic.LoadPointer(&c.tail)
	tail := (*node[T])(tailPtr)
	tailNext := atomic.LoadPointer(&tail.next)
	atomic.CompareAndSwapPointer(&tail.next, tailNext, newPtr)
	atomic.CompareAndSwapPointer(&c.tail, tailPtr, newPtr)
	return nil
}

func (c *ConcurrentLinkedQueue[T]) Dequeue() (T, error) {
	for {
		headPtr := atomic.LoadPointer(&c.head)
		head := (*node[T])(headPtr)
		tailPtr := atomic.LoadPointer(&c.tail)
		tail := (*node[T])(tailPtr)
		if head == tail {
			// 不需要做更多检测，在当下这一刻，我们就认为没有元素，即便这时候正好有人入队
			// 但是并不妨碍在它彻底入队完成——即所有的指针都调整好——之前，
			// 认为其实还是没有元素
			var t T
			return t, ErrEmptyQueue
		}
		headNextPtr := atomic.LoadPointer(&head.next)
		if atomic.CompareAndSwapPointer(&c.head, headPtr, headNextPtr) {
			headNext := (*node[T])(headNextPtr)
			return headNext.val, nil
		}
	}
}

func (c *ConcurrentLinkedQueue[T]) DequeueV1() (T, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	headPtr := atomic.LoadPointer(&c.head)
	head := (*node[T])(headPtr)
	tailPtr := atomic.LoadPointer(&c.tail)
	tail := (*node[T])(tailPtr)
	if head == tail {
		// 不需要做更多检测，在当下这一刻，我们就认为没有元素，即便这时候正好有人入队
		// 但是并不妨碍在它彻底入队完成——即所有的指针都调整好——之前，
		// 认为其实还是没有元素
		var t T
		return t, ErrEmptyQueue
	}
	headNextPtr := atomic.LoadPointer(&head.next)
	atomic.CompareAndSwapPointer(&c.head, headPtr, headNextPtr)
	headNext := (*node[T])(headNextPtr)
	return headNext.val, nil
}

type node[T any] struct {
	val  T
	wait int
	// *node[T]
	next unsafe.Pointer
}
