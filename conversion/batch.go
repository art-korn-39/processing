package conversion

import (
	"app/provider"
	"app/util"
)

type Batch map[int]*LinkedOperation

type LinkedOperation struct {
	Operation provider.Operation
	Next      *LinkedOperation
}

func (b Batch) Set(o provider.Operation) {

	val, ok := b[o.Id]
	if ok {
		for {
			if val.Operation.Transaction_completed_at_day.Equal(o.Transaction_completed_at_day) &&
				util.Equals(val.Operation.Channel_amount, o.Channel_amount) {
				return
			}
			if val.Next == nil { // дошли до последнего
				val.Next = &LinkedOperation{
					Operation: o,
					Next:      nil,
				}
				break
			}
			val = val.Next
		}
	} else {
		b[o.Id] = &LinkedOperation{
			Operation: o,
			Next:      nil,
		}
	}

}

func (b Batch) Get() []provider.Operation {

	res := make([]provider.Operation, 0, len(b)+100)

	for _, v := range b {
		for {
			res = append(res, v.Operation)
			if v.Next == nil {
				break
			}
			v = v.Next
		}
	}

	return res

}
