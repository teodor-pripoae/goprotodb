package db

import "unsafe"
import "C"

// Record identifier implementing the protobuf marshalling interfaces.
type RecordId uint32

// Marshal the record identifier. The result is machine dependent.
func (id *RecordId) Marshal() ([]byte, error) {
	return C.GoBytes(unsafe.Pointer(id), 4), nil
}

// Unmarshal the record identifier. The encoding is machine dependent.
func (id *RecordId) Unmarshal(data []byte) (err error) {
	if len(data) == 4 {
		*id = *(*RecordId)(unsafe.Pointer(&data[0]))
	} else {
		err = ErrInvalid
	}
	return
}
