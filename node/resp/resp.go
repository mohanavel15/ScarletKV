/*
MIT License

Copyright (c) 2024 Code & Learn

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Original source code available at: https://github.com/codeandlearn1991/go-redis-server/blob/d06a512599358fa082d62a040e5814e88a30517b/internal/resp/resp.go
*/

package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type DataType byte

const (
	Array        DataType = '*'
	BulkString   DataType = '$'
	Error        DataType = '-'
	Integer      DataType = ':'
	SimpleString DataType = '+'
)

type Value struct {
	Type    DataType
	String  string
	Integer int64
	Array   []*Value
	IsNull  bool
}

func readUntilCRLF(rd *bufio.Reader) ([]byte, error) {
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read line bytes: %w", err)
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("line not terminated with expected terminator")
	}

	return line[:len(line)-2], nil
}

func deserializeInteger(rd *bufio.Reader) (*Value, error) {
	// Example -> :1234\r\n
	d, err := readUntilCRLF(rd)
	if err != nil {
		return nil, fmt.Errorf("read integer data: %w", err)
	}

	i, err := strconv.ParseInt(string(d), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("deserialize integer value: %w", err)
	}

	return &Value{
		Type:    Integer,
		Integer: i,
	}, nil
}

func deserializeSimpleString(rd *bufio.Reader) (*Value, error) {
	// Example -> +Simple string\r\n
	d, err := readUntilCRLF(rd)
	if err != nil {
		return nil, fmt.Errorf("read simple string data: %w", err)
	}

	return &Value{
		Type:   SimpleString,
		String: string(d),
	}, nil
}

func deserializeError(rd *bufio.Reader) (*Value, error) {
	// Example -> -Error message here\r\n
	d, err := readUntilCRLF(rd)
	if err != nil {
		return nil, fmt.Errorf("read error data: %w", err)
	}

	return &Value{
		Type:   Error,
		String: string(d),
	}, nil
}

func deserializeBulkString(rd *bufio.Reader) (*Value, error) {
	// Example -> $5\r\nhello\r\n
	d, err := readUntilCRLF(rd)
	if err != nil {
		return nil, fmt.Errorf("read bulk string data: %w", err)
	}

	strLen, err := strconv.ParseInt(string(d), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse string len: %w", err)
	}

	if strLen == -1 {
		return &Value{
			Type:   BulkString,
			IsNull: true,
		}, nil
	}

	strBytes := make([]byte, strLen)
	readLen, err := io.ReadFull(rd, strBytes)
	if err != nil {
		return nil, fmt.Errorf("read bulk string: %w", err)
	}

	if readLen != int(strLen) {
		return nil, fmt.Errorf("short bulk string, expected: %d, got: %d", strLen, readLen)
	}

	crlf := make([]byte, 2)
	n, err := io.ReadFull(rd, crlf)
	if err != nil || n != 2 || crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, fmt.Errorf("bulk string not terminated correctly: %c", crlf)
	}

	return &Value{
		Type:   BulkString,
		String: string(strBytes),
	}, nil
}

func deserializeArray(rd *bufio.Reader) (*Value, error) {
	// Example -> *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
	d, err := readUntilCRLF(rd)
	if err != nil {
		return nil, fmt.Errorf("read num elements: %w", err)
	}

	n, err := strconv.ParseInt(string(d), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse the num elements: %w", err)
	}

	if n == -1 {
		return &Value{
			Type:   Array,
			IsNull: true,
		}, nil
	}

	arr := make([]*Value, n)

	for i := int64(0); i < n; i++ {
		arr[i], err = Deserilize(rd)
		if err != nil {
			return nil, fmt.Errorf("deserialize array element: %w", err)
		}
	}

	return &Value{
		Type:  Array,
		Array: arr,
	}, nil
}

func Deserilize(rd io.Reader) (*Value, error) {
	brd := bufio.NewReader(rd)

	respType, err := brd.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read resp first byte: %w", err)
	}

	switch DataType(respType) {
	case Array:
		return deserializeArray(brd)
	case BulkString:
		return deserializeBulkString(brd)
	case Integer:
		return deserializeInteger(brd)
	case SimpleString:
		return deserializeSimpleString(brd)
	case Error:
		return deserializeError(brd)
	default:
		return nil, fmt.Errorf("unknown resp type: %c", respType)
	}
}

func serializeSimpleString(v *Value) string {
	return "+" + v.String + "\r\n"
}

func serializeError(v *Value) string {
	return "-" + v.String + "\r\n"
}

func serializeInteger(v *Value) string {
	return ":" + strconv.Itoa(int(v.Integer)) + "\r\n"
}

func serializeBulkString(v *Value) string {
	if v.IsNull {
		return "$-1\r\n"
	}
	return "$" + strconv.Itoa(len(v.String)) + "\r\n" + v.String + "\r\n"
}

func serializeArray(v *Value) (string, error) {
	if v.IsNull {
		return "*-1\r\n", nil
	}

	var serialized string
	for _, el := range v.Array {
		s, err := Serialize(el)
		if err != nil {
			return "", fmt.Errorf("serializing error element: %w", err)
		}
		serialized += s
	}

	return "*" + strconv.Itoa(len(v.Array)) + "\r\n" + serialized, nil
}

func Serialize(v *Value) (string, error) {
	if v == nil {
		return "", errors.New("value is nil")
	}

	switch v.Type {
	case SimpleString:
		return serializeSimpleString(v), nil
	case Error:
		return serializeError(v), nil
	case Integer:
		return serializeInteger(v), nil
	case BulkString:
		return serializeBulkString(v), nil
	case Array:
		return serializeArray(v)
	default:
		return "", fmt.Errorf("invalid resp type: %v", v.Type)
	}
}

// NewError creates a new error resp value.
func NewError(msg string) *Value {
	return &Value{
		Type:   Error,
		String: msg,
	}
}

// NewSimpleString creates a new simple string resp value.
func NewSimpleString(msg string) *Value {
	return &Value{
		Type:   SimpleString,
		String: msg,
	}
}

func NewBulkString(msg string) *Value {
	// If string is empty, we want to return a null bulk string
	if msg == "" {
		return &Value{
			Type:   BulkString,
			IsNull: true,
		}
	}
	return &Value{
		Type:   BulkString,
		String: msg,
	}
}

// NewInteger creates a new integer resp value.
func NewInteger(num int64) *Value {
	return &Value{
		Type:    Integer,
		Integer: num,
	}
}

// NewArray creates a new array resp value.
func NewArray(vals ...*Value) *Value {
	// If no values, return a null array
	if len(vals) == 0 {
		return &Value{
			Type:   Array,
			IsNull: true,
		}
	}
	return &Value{
		Type:  Array,
		Array: vals,
	}
}
