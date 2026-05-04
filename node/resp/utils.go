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

func ProtoVal2RESP(value *ptypes.Value) *Value {
	new_value := Value{}
	new_value.IsNull = value.IsNull

	switch value.Type {
	case ptypes.ValueType_Array:
		new_value.Type = Array
		for _, v := range value.Array {
			new_value.Array = append(new_value.Array, ProtoVal2RESP(v))
		}
	case ptypes.ValueType_String:
		new_value.Type = BulkString
		new_value.String = value.String_
	case ptypes.ValueType_Number:
		new_value.Type = Integer
		new_value.Integer = value.Number
	}

	return &new_value
}
