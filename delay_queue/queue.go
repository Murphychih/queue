package delay_queue

import "errors"

var (
	ErrOutOfCapacity = errors.New("超出最大容量限制")
	ErrEmptyQueue    = errors.New("队列为空")
	ErrEnqueueQueue  = errors.New("争抢失败，请重新入队")
)
