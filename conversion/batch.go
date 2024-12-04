package conversion

import "app/provider_registry"

type Batch map[int]provider_registry.Operation

func (b Batch) Set(o provider_registry.Operation) {

	val, ok := b[o.Id]
	if ok {
		if o.Transaction_completed_at_day.After(val.Transaction_completed_at_day) {
			b[o.Id] = o
		}
	} else {
		b[o.Id] = o
	}

}

func (b Batch) SetSlice(ops []provider_registry.Operation) {

	for _, o := range ops {
		val, ok := b[o.Id]
		if ok {
			if o.Transaction_completed_at_day.After(val.Transaction_completed_at_day) {
				b[o.Id] = o
			}
		} else {
			b[o.Id] = o
		}
	}

}

func (b Batch) Get() []provider_registry.Operation {

	res := make([]provider_registry.Operation, 0, len(b)+100)

	for _, v := range b {
		res = append(res, v)
	}

	return res

}
