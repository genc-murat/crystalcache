package cache

import "github.com/genc-murat/crystalcache/internal/core/models"

func (c *MemoryCache) GetBit(key string, offset int64) (int, error) {
	return c.bitmapManager.GetBit(key, offset)
}

func (c *MemoryCache) SetBit(key string, offset int64, value int) (int, error) {
	return c.bitmapManager.SetBit(key, offset, value)
}

func (c *MemoryCache) BitCount(key string, start, end int64) (int64, error) {
	return c.bitmapManager.BitCount(key, start, end)
}

func (c *MemoryCache) BitPos(key string, bit int, start, end int64, reverse bool) (int64, error) {
	return c.bitmapManager.BitPos(key, bit, start, end, reverse)
}

func (c *MemoryCache) BitField(key string, commands []models.BitFieldCommand) ([]int64, error) {
	return c.bitmapManager.BitField(key, commands)
}

func (c *MemoryCache) BitFieldRO(key string, commands []models.BitFieldCommand) ([]int64, error) {
	return c.bitmapManager.BitFieldRO(key, commands)
}

func (c *MemoryCache) BitOp(operation string, destkey string, keys ...string) (int64, error) {
	return c.bitmapManager.BitOp(operation, destkey, keys...)
}
