package art

import (
	"math"
	"unsafe"
)

type nodeFactory interface {
	newNode4() *artNode
	newNode16() *artNode
	newNode48() *artNode
	newNode256() *artNode
	newLeaf(key Key, value interface{}) *artNode
}

// make sure that objFactory implements all methods of nodeFactory interface
var _ nodeFactory = &objFactory{}

//var factory = newObjFactory()

var factory = newArenaFactory()

func newTree() *tree {
	return &tree{}
}

type objFactory struct{}

func newObjFactory() nodeFactory {
	return &objFactory{}
}

// Simple obj factory implementation
func (f *objFactory) newNode4() *artNode {
	return &artNode{kind: Node4, ref: unsafe.Pointer(new(node4))}
}

func (f *objFactory) newNode16() *artNode {
	return &artNode{kind: Node16, ref: unsafe.Pointer(&node16{})}
}

func (f *objFactory) newNode48() *artNode {
	return &artNode{kind: Node48, ref: unsafe.Pointer(&node48{})}
}

func (f *objFactory) newNode256() *artNode {
	return &artNode{kind: Node256, ref: unsafe.Pointer(&node256{})}
}

func (f *objFactory) newLeaf(key Key, value interface{}) *artNode {
	clonedKey := make(Key, len(key))
	copy(clonedKey, key)
	return &artNode{
		kind: Leaf,
		ref:  unsafe.Pointer(&leaf{key: clonedKey, value: value}),
	}
}

type memdbArenaBlock struct {
	buf    []byte
	length int
}

type memdbArenaAddr struct {
	idx uint32
	off uint32
}

func (addr memdbArenaAddr) isNull() bool {
	if addr == nullAddr {
		return true
	}
	if addr.idx == math.MaxUint32 || addr.off == math.MaxUint32 {
		// defensive programming, the code should never run to here.
		// it always means something wrong... (maybe caused by data race?)
		// because we never set part of idx/off to math.MaxUint64
		return true
	}
	return false
}

const (
	alignMask       = 1<<32 - 8 // 29 bit 1 and 3 bit 0.
	nullBlockOffset = math.MaxUint32

	maxBlockSize  = 128 << 20
	initBlockSize = 4 * 1024
)

var (
	nullAddr = memdbArenaAddr{math.MaxUint32, math.MaxUint32}
)

type arenaFactory struct {
	blockSize int
	blocks    []memdbArenaBlock
	// the total size of all blocks, also the approximate memory footprint of the arena.
	capacity uint64
}

func newArenaFactory() *arenaFactory {
	return &arenaFactory{
		blockSize: 0,
		blocks:    nil,
		capacity:  0,
	}
}

func (f *arenaFactory) enlarge(allocSize, blockSize int) {
	f.blockSize = blockSize
	for f.blockSize <= allocSize {
		f.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if f.blockSize > maxBlockSize {
		f.blockSize = maxBlockSize
	}
	f.blocks = append(f.blocks, memdbArenaBlock{
		buf: make([]byte, f.blockSize),
	})
	f.capacity += uint64(f.blockSize)
}

func (f *arenaFactory) alloc(size int) (memdbArenaAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(f.blocks) == 0 {
		f.enlarge(size, initBlockSize)
	}

	addr, data := f.allocInLastBlock(size, true)
	if !addr.isNull() {
		return addr, data
	}

	f.enlarge(size, f.blockSize<<1)
	return f.allocInLastBlock(size, true)
}

func (f *arenaFactory) allocInLastBlock(size int, align bool) (memdbArenaAddr, []byte) {
	idx := len(f.blocks) - 1
	offset, data := f.blocks[idx].alloc(size, align)
	if offset == nullBlockOffset {
		return nullAddr, nil
	}
	return memdbArenaAddr{uint32(idx), offset}, data
}

func (a *memdbArenaBlock) alloc(size int, align bool) (uint32, []byte) {
	offset := a.length
	if align {
		// We must align the allocated address for node
		// to make runtime.checkptrAlignment happy.
		offset = (a.length + 7) & alignMask
	}
	newLen := offset + size
	if newLen > len(a.buf) {
		return nullBlockOffset, nil
	}
	a.length = newLen
	return uint32(offset), a.buf[offset : offset+size]
}

const (
	artNodeLen = 16
	node4Len   = artNodeLen + 64
	node16Len  = artNodeLen + 176
	node48Len  = artNodeLen + 696
	node256Len = artNodeLen + 2072
)

func (f *arenaFactory) newNode4() *artNode {
	addr, data := f.alloc(node4Len)
	if addr.isNull() {
		panic("addr is null")
	}
	anData := data[:artNodeLen]
	data = data[artNodeLen:]
	an := (*artNode)(unsafe.Pointer(&anData))
	n4 := (*node4)(unsafe.Pointer(&data))
	n4.zeroChild = nil
	an.kind = Node4
	an.ref = unsafe.Pointer(n4)
	return an
}

func (f *arenaFactory) newNode16() *artNode {
	addr, data := f.alloc(node16Len)
	if addr.isNull() {
		panic("addr is null")
	}
	anData := data[:artNodeLen]
	data = data[artNodeLen:]
	an := (*artNode)(unsafe.Pointer(&anData))
	an.kind = Node16
	an.ref = unsafe.Pointer(&data)
	return an
}

func (f *arenaFactory) newNode48() *artNode {
	addr, data := f.alloc(node48Len)
	if addr.isNull() {
		panic("addr is null")
	}
	anData := data[:artNodeLen]
	data = data[artNodeLen:]
	an := (*artNode)(unsafe.Pointer(&anData))
	an.kind = Node48
	an.ref = unsafe.Pointer(&data)
	return an
}

func (f *arenaFactory) newNode256() *artNode {
	addr, data := f.alloc(node256Len)
	if addr.isNull() {
		panic("addr is null")
	}
	anData := data[:artNodeLen]
	data = data[artNodeLen:]
	an := (*artNode)(unsafe.Pointer(&anData))
	an.kind = Node256
	an.ref = unsafe.Pointer(&data)
	return an
}

func (f *arenaFactory) newLeaf(key Key, value interface{}) *artNode {
	clonedKey := make(Key, len(key))
	copy(clonedKey, key)
	return &artNode{
		kind: Leaf,
		ref:  unsafe.Pointer(&leaf{key: clonedKey, value: value}),
	}
}
