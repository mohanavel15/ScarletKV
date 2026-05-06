package main

import (
	"node/ptypes"
	"node/resp"
)

func RESP2ProtoVal(value *resp.Value) *ptypes.Value {
	var pval = ptypes.Value{}
	pval.IsNull = value.IsNull

	switch value.Type {
	case resp.Array:
		pval.Type = ptypes.ValueType_Array
		for _, v := range value.Array {
			pval.Array = append(pval.Array, RESP2ProtoVal(v))
		}
	case resp.SimpleString:
	case resp.BulkString:
		pval.Type = ptypes.ValueType_String
		pval.String_ = value.String
	case resp.Integer:
		pval.Type = ptypes.ValueType_Number
		pval.Number = value.Integer
	case resp.Error:
		panic("RESP2ProtoVal: Value Type Error!")
	}

	return &pval
}

func ProtoVal2RESP(value *ptypes.Value) *resp.Value {
	new_value := resp.Value{}
	new_value.IsNull = value.IsNull

	switch value.Type {
	case ptypes.ValueType_Array:
		new_value.Type = resp.Array
		for _, v := range value.Array {
			new_value.Array = append(new_value.Array, ProtoVal2RESP(v))
		}
	case ptypes.ValueType_String:
		new_value.Type = resp.BulkString
		new_value.String = value.String_
	case ptypes.ValueType_Number:
		new_value.Type = resp.Integer
		new_value.Integer = value.Number
	}

	return &new_value
}
