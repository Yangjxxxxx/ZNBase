package quotapool

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestNodeSize 确保节点的字节大小符合预期
func TestNodeSize(t *testing.T) {
	assert.Equal(t, 32+8*bufferSize, int(unsafe.Sizeof(node{})))
}
