package ports

import "github.com/genc-murat/crystalcache/internal/core/models"

type Storage interface {
	Write(value models.Value) error
	Read(callback func(value models.Value)) error
	Close() error
}
