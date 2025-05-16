package sverka

import (
	"app/convert"
	"app/logs"
	"app/processing_provider"
	pg "app/provider_registry"
	"fmt"
	"strconv"
	"time"
)

type differ_struct struct {
	Provider_operation *pg.Operation
	Bof_operation      *convert.Bof_operation
	Detailed_operation *processing_provider.Detailed_row
}

func compare(full_registry []*convert.Base_operation, detailed detailed_struct) (result []differ_struct) {

	start_time := time.Now()
	cntWithoutDetailed := 0
	cntDifferent := 0
	result = []differ_struct{}

	for _, base_operation := range full_registry {

		var ok bool

		differ := differ_struct{
			Provider_operation: base_operation.Provider_operation,
			Bof_operation:      base_operation.Bof_operation,
		}

		switch base_operation.Setting.Key_column {
		case convert.OPID:
			differ.Detailed_operation, ok = detailed.opid_map[strconv.Itoa(base_operation.Provider_operation.Id)]
		case convert.PAYID:
			differ.Detailed_operation, ok = detailed.payid_map[base_operation.Provider_operation.Provider_payment_id]
		}

		if !ok {
			cntWithoutDetailed++
			continue
		}

		prov_op := differ.Provider_operation
		bof_op := differ.Bof_operation
		det_op := differ.Detailed_operation

		if bof_op == nil {
			continue
		}

		if prov_op.Channel_amount != 0 && bof_op.Channel_amount != 0 && det_op.Channel_amount != 0 {

			if prov_op.Channel_amount != bof_op.Channel_amount || prov_op.Channel_amount != det_op.Channel_amount {
				result = append(result, differ)
				cntDifferent++
				continue
			}
		}

		// if !prov_op.Transaction_created_at.Equal(bof_op.Transaction_created_at) || !prov_op.Transaction_created_at.Equal(det_op.Transaction_created_at) {
		// 	result = append(result, differ)
		// 	cntDifferent++
		// 	continue
		// }

		if prov_op.BR_amount != 0 && det_op.BR_balance_currency != 0 {

			if prov_op.BR_amount != det_op.BR_balance_currency {
				result = append(result, differ)
				cntDifferent++
				continue
			}
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Сверка по основным полям: %v [отличаются: %d, без detailed: %d]", time.Since(start_time), cntDifferent, cntWithoutDetailed))

	return

}
