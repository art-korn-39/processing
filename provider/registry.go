package provider

import (
	"app/util"
	"time"
)

type registry map[int]*LinkedOperation

type LinkedOperation struct {
	Operation *Operation
	Next      *LinkedOperation
}

func (r registry) Set(o Operation) {

	val, ok := r[o.Id]
	if ok {
		for {
			if val.Next == nil { // дошли до последнего
				val.Next = &LinkedOperation{
					Operation: &o,
					Next:      nil,
				}
				break
			}
			val = val.Next
		}
	} else {
		r[o.Id] = &LinkedOperation{
			Operation: &o,
			Next:      nil,
		}
	}

}

func (r registry) Get(id int, d time.Time, amount float64) (*Operation, bool) {

	val, ok := r[id]
	if ok {
		for {
			op := val.Operation
			if op.Transaction_completed_at_day.Equal(d) && util.Equals(op.Channel_amount, amount) {
				return op, true
			}

			if val.Next == nil {
				break
			}
			val = val.Next
		}
	}

	return nil, false

}
