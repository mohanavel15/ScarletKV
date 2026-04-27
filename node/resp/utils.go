package resp

import "node/ptypes"

func RESP2ProtoVal(value *Value) *ptypes.Value {
	var pval = ptypes.Value{}
	pval.IsNull = value.IsNull

	switch value.Type {
	case Array:
		pval.Type = ptypes.ValueType_Array
		for _, v := range value.Array {
			pval.Array = append(pval.Array, RESP2ProtoVal(v))
		}
	case SimpleString:
	case BulkString:
		pval.Type = ptypes.ValueType_String
		pval.String_ = value.String
	case Integer:
		pval.Type = ptypes.ValueType_Number
		pval.Number = value.Integer
	case Error:
		panic("RESP2ProtoVal: Value Type Error!")
	}

	return &pval
}
