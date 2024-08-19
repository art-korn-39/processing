package conversion

import "app/provider"

type Batch2 map[int]provider.Operation

func (b Batch2) Set(o provider.Operation) {

	val, ok := b[o.Id]
	if ok {
		if o.Transaction_completed_at_day.After(val.Transaction_completed_at_day) {
			b[o.Id] = o
		}
	} else {
		b[o.Id] = o
	}

}

func (b Batch2) Get() []provider.Operation {

	res := make([]provider.Operation, 0, len(b)+100)

	for _, v := range b {
		res = append(res, v)
	}

	return res

}
