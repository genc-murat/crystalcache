package bitmap

import (
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type Manager struct {
	basicOps   *BasicOps
	countOps   *CountOps
	fieldOps   *FieldOps
	bitwiseOps *BitwiseOps
}

func NewManager(bcache *sync.Map, version *sync.Map) *Manager {

	basicOps := NewBasicOps(bcache, version)

	return &Manager{
		basicOps:   basicOps,
		countOps:   NewCountOps(basicOps),
		fieldOps:   NewFieldOps(basicOps),
		bitwiseOps: NewBitwiseOps(basicOps),
	}
}

func (m *Manager) GetBit(key string, offset int64) (int, error) {
	return m.basicOps.GetBit(key, offset)
}

func (m *Manager) SetBit(key string, offset int64, value int) (int, error) {
	return m.basicOps.SetBit(key, offset, value)
}

func (m *Manager) BitCount(key string, start, end int64) (int64, error) {
	return m.countOps.BitCount(key, start, end)
}

func (m *Manager) BitPos(key string, bit int, start, end int64, reverse bool) (int64, error) {
	return m.countOps.BitPos(key, bit, start, end, reverse)
}

func (m *Manager) BitField(key string, commands []models.BitFieldCommand) ([]int64, error) {
	return m.fieldOps.BitField(key, commands)
}

func (m *Manager) BitFieldRO(key string, commands []models.BitFieldCommand) ([]int64, error) {
	return m.fieldOps.BitFieldRO(key, commands)
}

func (m *Manager) BitOp(operation string, destkey string, keys ...string) (int64, error) {
	return m.bitwiseOps.BitOp(operation, destkey, keys...)
}

func (m *Manager) GetBitmap(key string) []byte {
	return m.basicOps.GetBitmap(key)
}
