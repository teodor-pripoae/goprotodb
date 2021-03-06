// Code generated by protoc-gen-go.
// source: records_test.proto
// DO NOT EDIT!

package protodb

import proto "code.google.com/p/goprotobuf/proto"
import json "encoding/json"
import math "math"

// Reference proto, json, and math imports to suppress error if they are not otherwise used.
var _ = proto.Marshal
var _ = &json.SyntaxError{}
var _ = math.Inf

type TestRecord struct {
	Key              *TestRecord_Key `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Val              *string         `protobuf:"bytes,2,req,name=val" json:"val,omitempty"`
	XXX_unrecognized []byte          `json:"-"`
}

func (this *TestRecord) Reset()         { *this = TestRecord{} }
func (this *TestRecord) String() string { return proto.CompactTextString(this) }
func (*TestRecord) ProtoMessage()       {}

func (this *TestRecord) GetKey() *TestRecord_Key {
	if this != nil {
		return this.Key
	}
	return nil
}

func (this *TestRecord) GetVal() string {
	if this != nil && this.Val != nil {
		return *this.Val
	}
	return ""
}

type TestRecord_Key struct {
	Val              *string `protobuf:"bytes,1,req,name=val" json:"val,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *TestRecord_Key) Reset()         { *this = TestRecord_Key{} }
func (this *TestRecord_Key) String() string { return proto.CompactTextString(this) }
func (*TestRecord_Key) ProtoMessage()       {}

func (this *TestRecord_Key) GetVal() string {
	if this != nil && this.Val != nil {
		return *this.Val
	}
	return ""
}

type NumberedTestRecord struct {
	Key              *uint32 `protobuf:"fixed32,1,opt,name=key" json:"key,omitempty"`
	Val              *string `protobuf:"bytes,2,req,name=val" json:"val,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (this *NumberedTestRecord) Reset()         { *this = NumberedTestRecord{} }
func (this *NumberedTestRecord) String() string { return proto.CompactTextString(this) }
func (*NumberedTestRecord) ProtoMessage()       {}

func (this *NumberedTestRecord) GetKey() uint32 {
	if this != nil && this.Key != nil {
		return *this.Key
	}
	return 0
}

func (this *NumberedTestRecord) GetVal() string {
	if this != nil && this.Val != nil {
		return *this.Val
	}
	return ""
}

func init() {
}
