package util

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func ValidateArgs(args []models.Value, count int) error {
	if len(args) != count {
		return fmt.Errorf("ERR wrong number of arguments")
	}
	return nil
}

func ValidateMinArgs(args []models.Value, minCount int) error {
	if len(args) < minCount {
		return fmt.Errorf("ERR wrong number of arguments")
	}
	return nil
}

func ValidateKeyArg(args []models.Value) error {
	if len(args) < 1 {
		return fmt.Errorf("ERR no key specified")
	}
	return nil
}

func ValidateKeyValue(args []models.Value) error {
	if len(args) < 2 {
		return fmt.Errorf("ERR no value specified")
	}
	return nil
}
