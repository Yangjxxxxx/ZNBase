package tree

import (
	"bytes"
	"crypto/aes"
	"unsafe"
)

// AesTool for ENCRYPTION and decryption of AES-128-ECB mode
type AesTool struct {
	//The length corresponding to 128, 192, 256 bits is 16, 24, 32, bytes respectively
	Key       []byte
	BlockSize int
}

// NewAesTool for construction
func NewAesTool(key []byte, blockSize int) *AesTool {
	return &AesTool{Key: key, BlockSize: blockSize}
}

// padding
func (*AesTool) padding(src []byte) []byte {
	paddingCount := aes.BlockSize - len(src)%aes.BlockSize
	if paddingCount == 0 {
		return src
	}
	//填充数据
	return append(src, bytes.Repeat([]byte{byte(0)}, paddingCount)...)
}

// unPadding
func (*AesTool) unPadding(src []byte) []byte {
	for i := len(src) - 1; i >= 0; i-- {
		if src[i] != 0 {
			return src[:i+1]
		}
	}
	var temp []byte
	return temp
}

// Encrypt encrypts string
func (aesTool *AesTool) Encrypt(src []byte) ([]byte, error) {
	block, err := aes.NewCipher(aesTool.Key)
	if err != nil {
		return nil, err
	}
	src = aesTool.padding(src)
	var encryptData []byte
	tmpData := make([]byte, aesTool.BlockSize)
	for index := 0; index < len(src); index += aesTool.BlockSize {
		block.Encrypt(tmpData, src[index:index+aesTool.BlockSize])
		for _, v := range tmpData {
			encryptData = append(encryptData, v)
		}
	}
	return encryptData, nil
}

// Decrypt decrypts code
func (aesTool *AesTool) Decrypt(src []byte) ([]byte, error) {
	//The key is only 16, 24, 32
	block, err := aes.NewCipher(aesTool.Key)
	if err != nil {
		return nil, err
	}
	//Return encryption result
	var decryptData []byte
	//Store encrypted data at a time
	tmpData := make([]byte, aesTool.BlockSize)
	//Packet block encryption
	for index := 0; index < len(src); index += aesTool.BlockSize {
		block.Decrypt(tmpData, src[index:index+aesTool.BlockSize])
		for _, v := range tmpData {
			decryptData = append(decryptData, v)
		}
	}
	return aesTool.unPadding(decryptData), nil
}

// Encrypt encrypts
func Encrypt(p, key []byte) []byte {
	blickSize := 16
	tool := NewAesTool(key, blickSize)
	tool.Key = tool.padding(key)
	encryptData, _ := tool.Encrypt(p)
	return encryptData
}

// Decrypt decrypts
func Decrypt(p, key []byte) []byte {
	blickSize := 16
	tool := NewAesTool(key, blickSize)
	tool.Key = tool.padding(key)
	decryptData, _ := tool.Decrypt(p)
	return decryptData
}

// Str2bytes convert string to bytes
func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
