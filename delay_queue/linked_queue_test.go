package delay_queue

import (
	"fmt"
	"math/rand"
	"queue/gopool"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const benchmarkTimes = 10000

func TestConcurrentQueue_Enqueue(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		q    func() *ConcurrentLinkedQueue[int]
		val  int

		wantData []int
		wantErr  error
	}{
		{
			name: "empty",
			q: func() *ConcurrentLinkedQueue[int] {
				return NewConcurrentLinkedQueue[int]()
			},
			val:      123,
			wantData: []int{123},
		},
		{
			name: "multiple",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.Enqueue(123)
				require.NoError(t, err)
				return q
			},
			val:      234,
			wantData: []int{123, 234},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.q()
			err := q.Enqueue(tc.val)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantData, q.asSlice())
		})
	}
}

func TestConcurrentQueue_EnqueueV1(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		q    func() *ConcurrentLinkedQueue[int]
		val  int

		wantData []int
		wantErr  error
	}{
		{
			name: "empty",
			q: func() *ConcurrentLinkedQueue[int] {
				return NewConcurrentLinkedQueue[int]()
			},
			val:      123,
			wantData: []int{123},
		},
		{
			name: "multiple",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.EnqueueV1(123)
				require.NoError(t, err)
				return q
			},
			val:      234,
			wantData: []int{123, 234},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.q()
			err := q.EnqueueV1(tc.val)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantData, q.asSlice())
		})
	}
}

func TestConcurrentQueue_Dequeue(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		q        func() *ConcurrentLinkedQueue[int]
		wantVal  int
		wantData []int
		wantErr  error
	}{
		{
			name: "empty",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				return q
			},
			wantErr: ErrEmptyQueue,
		},
		{
			name: "single",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.Enqueue(123)
				assert.NoError(t, err)
				return q
			},
			wantVal: 123,
		},
		{
			name: "multiple",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.Enqueue(123)
				assert.NoError(t, err)
				err = q.Enqueue(234)
				assert.NoError(t, err)
				return q
			},
			wantVal:  123,
			wantData: []int{234},
		},
		{
			name: "enqueue and dequeue",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.Enqueue(123)
				assert.NoError(t, err)
				err = q.Enqueue(234)
				assert.NoError(t, err)
				val, err := q.Dequeue()
				assert.Equal(t, 123, val)
				assert.NoError(t, err)
				err = q.Enqueue(345)
				assert.NoError(t, err)
				return q
			},
			wantVal:  234,
			wantData: []int{345},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.q()
			val, err := q.Dequeue()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, tc.wantData, q.asSlice())
		})
	}
}

func TestConcurrentQueue_DequeueV1(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		q        func() *ConcurrentLinkedQueue[int]
		wantVal  int
		wantData []int
		wantErr  error
	}{
		{
			name: "empty",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				return q
			},
			wantErr: ErrEmptyQueue,
		},
		{
			name: "single",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.EnqueueV1(123)
				assert.NoError(t, err)
				return q
			},
			wantVal: 123,
		},
		{
			name: "multiple",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.EnqueueV1(123)
				assert.NoError(t, err)
				err = q.EnqueueV1(234)
				assert.NoError(t, err)
				return q
			},
			wantVal:  123,
			wantData: []int{234},
		},
		{
			name: "enqueue and dequeue",
			q: func() *ConcurrentLinkedQueue[int] {
				q := NewConcurrentLinkedQueue[int]()
				err := q.EnqueueV1(123)
				assert.NoError(t, err)
				err = q.EnqueueV1(234)
				assert.NoError(t, err)
				val, err := q.DequeueV1()
				assert.Equal(t, 123, val)
				assert.NoError(t, err)
				err = q.EnqueueV1(345)
				assert.NoError(t, err)
				return q
			},
			wantVal:  234,
			wantData: []int{345},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.q()
			val, err := q.DequeueV1()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, tc.wantData, q.asSlice())
		})
	}
}

func TestConcurrentLinkedQueue(t *testing.T) {
	t.Parallel()
	// 仅仅是为了测试在入队出队期间不会出现 panic 或者死循环之类的问题
	q := NewConcurrentLinkedQueue[int]()
	var wg sync.WaitGroup
	wg.Add(10000)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				val := rand.Int()
				_ = q.Enqueue(val)
			}
		}()
	}
	var cnt int32 = 0
	for i := 0; i < 10; i++ {
		go func() {
			for {
				if atomic.LoadInt32(&cnt) >= 10000 {
					return
				}
				_, err := q.Dequeue()
				if err == nil {
					atomic.AddInt32(&cnt, 1)
					wg.Done()
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentLinkedQueueV1(t *testing.T) {
	t.Parallel()
	// 仅仅是为了测试在入队出队期间不会出现 panic 或者死循环之类的问题
	q := NewConcurrentLinkedQueue[int]()
	var wg sync.WaitGroup
	wg.Add(10000)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				val := rand.Int()
				_ = q.EnqueueV1(val)
			}
		}()
	}
	var cnt int32 = 0
	for i := 0; i < 10; i++ {
		go func() {
			for {
				if atomic.LoadInt32(&cnt) >= 10000 {
					return
				}
				_, err := q.DequeueV1()
				if err == nil {
					atomic.AddInt32(&cnt, 1)
					wg.Done()
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkQueue(b *testing.B) {
	q := NewConcurrentLinkedQueue[int]()
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	runtime.GOMAXPROCS(16) // 控制 P 的数量
	wg.Add(2 * benchmarkTimes)
	for i := 0; i < benchmarkTimes; i++ {
		val := i // 使用固定的元素值
		go func() {
			for j := 0; j < b.N; j++ {
				_ = q.Enqueue(val)
			}
			wg.Done()
		}()
		go func() {
			for j := 0; j < b.N; j++ {
				_, _ = q.Dequeue()
				// 模拟爬虫逻辑
				time.Sleep(500 * time.Microsecond)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportAllocs() // 在 b.ResetTimer() 后使用 b.ReportAllocs()
	fmt.Printf("miss的次数 Enqueue:%v, Dequeue:%v \n", q.missEnqueue, q.missDnqueue)
}

func BenchmarkQueueWithPool(b *testing.B) {
	q := NewConcurrentLinkedQueue[int]()
	var wg sync.WaitGroup
	config := gopool.NewConfig()
	config.ScaleThreshold = 1
	p := gopool.NewPool("benchmark", int32(runtime.GOMAXPROCS(0)), config)
	b.ReportAllocs()
	b.ResetTimer()
	runtime.GOMAXPROCS(16) // 控制 P 的数量
	wg.Add(2 * benchmarkTimes)
	for i := 0; i < benchmarkTimes; i++ {
		val := i // 使用固定的元素值
		p.Go(func() {
			for j := 0; j < b.N; j++ {
				_ = q.Enqueue(val)
			}
			wg.Done()
		})
		p.Go(func() {
			for j := 0; j < b.N; j++ {
				_, _ = q.Dequeue()
				// 模拟爬虫逻辑
				time.Sleep(500 * time.Microsecond)
			}
			wg.Done()
		})
	}
	wg.Wait()
	b.StopTimer()
	b.ReportAllocs() // 在 b.ResetTimer() 后使用 b.ReportAllocs()
	fmt.Printf("miss的次数 Enqueue:%v, Dequeue:%v \n", q.missEnqueue, q.missDnqueue)
}

// 这是最快的
func BenchmarkQueueV1(b *testing.B) {
	q := NewConcurrentLinkedQueue[int]()
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	runtime.GOMAXPROCS(16) // 控制 goroutine 的数量
	wg.Add(2 * benchmarkTimes)
	for i := 0; i < benchmarkTimes; i++ {
		val := i // 使用固定的元素值
		go func() {
			for j := 0; j < b.N; j++ {
				_ = q.EnqueueV1(val)
			}
			wg.Done()
		}()
		go func() {
			for j := 0; j < b.N; j++ {
				_, _ = q.DequeueV1()
				// 模拟爬虫逻辑
				time.Sleep(500 * time.Microsecond)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportAllocs() // 在 b.ResetTimer() 后使用 b.ReportAllocs()
}

func BenchmarkQueueV1WithPool(b *testing.B) {
	q := NewConcurrentLinkedQueue[int]()
	config := gopool.NewConfig()
	config.ScaleThreshold = 1
	p := gopool.NewPool("benchmark", int32(runtime.GOMAXPROCS(0)), config)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	runtime.GOMAXPROCS(16) // 控制 goroutine 的数量
	wg.Add(2 * benchmarkTimes)
	for i := 0; i < benchmarkTimes; i++ {
		val := i // 使用固定的元素值
		p.Go(func() {
			for j := 0; j < b.N; j++ {
				_ = q.EnqueueV1(val)
			}
			wg.Done()
		})
		p.Go(func() {
			for j := 0; j < b.N; j++ {
				_, _ = q.DequeueV1()
				// 模拟爬虫逻辑
				time.Sleep(500 * time.Microsecond)
			}
			wg.Done()
		})
	}
	wg.Wait()
	b.StopTimer()
	b.ReportAllocs() // 在 b.ResetTimer() 后使用 b.ReportAllocs()
}

func (c *ConcurrentLinkedQueue[T]) asSlice() []T {
	var res []T
	cur := (*node[T])((*node[T])(c.head).next)
	for cur != nil {
		res = append(res, cur.val)
		fmt.Printf("cur %v", cur.val)
		cur = (*node[T])(cur.next)
	}
	return res
}

func ExampleNewConcurrentLinkedQueue() {
	q := NewConcurrentLinkedQueue[int]()
	_ = q.Enqueue(10)
	val, err := q.Dequeue()
	if err != nil {
		// 一般意味着队列为空
		fmt.Println(err)
	}
	fmt.Println(val)
	// Output:
	// 10
}
