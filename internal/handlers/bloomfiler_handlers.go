package handlers

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type BloomFilterHandlers struct {
	cache ports.Cache
}

// NewBloomFilterHandlers creates a new instance of BloomFilterHandlers with the provided cache.
// It takes a single parameter:
// - cache: an implementation of the ports.Cache interface.
// It returns a pointer to a BloomFilterHandlers struct.
func NewBloomFilterHandlers(cache ports.Cache) *BloomFilterHandlers {
	return &BloomFilterHandlers{
		cache: cache,
	}
}

// HandleBFAdd handles the 'BF.ADD' command which adds an element to the Bloom filter.
// It expects exactly two arguments: the key of the Bloom filter and the element to add.
// If the number of arguments is incorrect, it returns an error.
// It returns an integer value indicating whether the element was added (1) or already present (0).
// In case of an error during the addition, it returns an error message.
func (h *BloomFilterHandlers) HandleBFAdd(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.ADD' command",
		}
	}

	added, err := h.cache.BFAdd(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  boolToInt(added),
	}
}

// HandleBFInsert handles the 'BF.INSERT' command for inserting items into a Bloom filter.
// It expects at least 4 arguments: the key, error rate, capacity, and one or more items to insert.
//
// Args:
//
//	args ([]models.Value): A slice of Value objects representing the command arguments.
//	  - args[0]: The key for the Bloom filter.
//	  - args[1]: The error rate for the Bloom filter (must be a float between 0 and 1).
//	  - args[2]: The capacity of the Bloom filter (must be a positive integer).
//	  - args[3...]: The items to insert into the Bloom filter.
//
// Returns:
//
//	models.Value: A Value object representing the result of the insertion.
//	  - If the number of arguments is less than 4, returns an error Value with a message indicating the wrong number of arguments.
//	  - If the error rate is invalid, returns an error Value with a message indicating the error rate must be between 0 and 1.
//	  - If the capacity is invalid, returns an error Value with a message indicating the capacity must be a positive integer.
//	  - If the insertion is successful, returns an array Value containing integer Values indicating whether each item was added (1) or already existed (0).
//	  - If there is an error during insertion, returns an error Value with the error message.
func (h *BloomFilterHandlers) HandleBFInsert(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.INSERT' command",
		}
	}

	errorRate, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid error rate. Must be between 0 and 1",
		}
	}

	capacity, err := strconv.ParseUint(args[2].Bulk, 10, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid capacity. Must be a positive integer",
		}
	}

	items := make([]string, len(args)-3)
	for i := 3; i < len(args); i++ {
		items[i-3] = args[i].Bulk
	}

	results, err := h.cache.BFInsert(args[0].Bulk, errorRate, uint(capacity), items)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	response := make([]models.Value, len(results))
	for i, added := range results {
		response[i] = models.Value{
			Type: "integer",
			Num:  boolToInt(added),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFExists handles the 'BF.EXISTS' command for checking the existence of an element in a Bloom filter.
// It expects exactly two arguments: the key of the Bloom filter and the element to check for existence.
// If the number of arguments is incorrect, it returns an error.
// It returns an integer value indicating whether the element exists in the Bloom filter (1 if exists, 0 if not).
// In case of an error during the existence check, it returns an error message.
func (h *BloomFilterHandlers) HandleBFExists(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.EXISTS' command",
		}
	}

	exists, err := h.cache.BFExists(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  boolToInt(exists),
	}
}

// HandleBFReserve handles the 'BF.RESERVE' command which initializes a Bloom Filter with a specified error rate and capacity.
// It expects three arguments: the key for the Bloom Filter, the desired error rate (a float between 0 and 1), and the capacity (a positive integer).
// If the number of arguments is incorrect, or if the error rate or capacity are invalid, it returns an error.
// On successful initialization, it returns an "OK" string.
//
// Args:
//
//	args ([]models.Value): A slice of Value objects representing the command arguments.
//
// Returns:
//
//	models.Value: A Value object indicating the result of the command execution.
func (h *BloomFilterHandlers) HandleBFReserve(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.RESERVE' command",
		}
	}

	errorRate, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid error rate. Must be between 0 and 1",
		}
	}

	capacity, err := strconv.ParseUint(args[2].Bulk, 10, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid capacity. Must be a positive integer",
		}
	}

	err = h.cache.BFReserve(args[0].Bulk, errorRate, uint(capacity))
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "string",
		Str:  "OK",
	}
}

// HandleBFMAdd handles the 'BF.MADD' command which adds multiple items to a Bloom filter.
// It expects at least two arguments: the key of the Bloom filter and one or more items to add.
// If the number of arguments is less than two, it returns an error.
// It returns an array of integers indicating whether each item was added to the Bloom filter (1 if added, 0 if already present).
// In case of an error during the addition process, it returns an error message.
//
// Args:
//
//	args ([]models.Value): The command arguments where the first argument is the key and the subsequent arguments are the items to add.
//
// Returns:
//
//	models.Value: An array of integers indicating the result of each addition or an error message if the command fails.
func (h *BloomFilterHandlers) HandleBFMAdd(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.MADD' command",
		}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.BFMAdd(key, items)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	response := make([]models.Value, len(results))
	for i, added := range results {
		response[i] = models.Value{
			Type: "integer",
			Num:  boolToInt(added),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFMExists handles the 'BF.MEXISTS' command which checks for the existence of multiple items in a Bloom filter.
// It expects at least two arguments: the key of the Bloom filter and one or more items to check for existence.
// If the number of arguments is less than 2, it returns an error.
// It returns an array of integers where each integer represents whether the corresponding item exists in the Bloom filter (1 if exists, 0 if not).
// In case of an error during the existence check, it returns an error message.
//
// Args:
//
//	args ([]models.Value): The command arguments where the first argument is the key and the subsequent arguments are the items to check.
//
// Returns:
//
//	models.Value: An array of integers indicating the existence of each item or an error message if the command fails.
func (h *BloomFilterHandlers) HandleBFMExists(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.MEXISTS' command",
		}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.BFMExists(key, items)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	response := make([]models.Value, len(results))
	for i, exists := range results {
		response[i] = models.Value{
			Type: "integer",
			Num:  boolToInt(exists),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFInfo processes the 'BF.INFO' command and returns information about a Bloom filter.
// It expects a single argument which is the key of the Bloom filter.
//
// Args:
//
//	args ([]models.Value): A slice containing the arguments for the command.
//
// Returns:
//
//	models.Value: A value containing the Bloom filter information in an array of key-value pairs,
//	              or an error message if the number of arguments is incorrect or if there is an error
//	              retrieving the Bloom filter information.
func (h *BloomFilterHandlers) HandleBFInfo(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.INFO' command",
		}
	}

	info, err := h.cache.BFInfo(args[0].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	// Convert map to array of key-value pairs
	response := make([]models.Value, 0, len(info)*2)
	for k, v := range info {
		response = append(response,
			models.Value{Type: "bulk", Bulk: k},
			models.Value{Type: "bulk", Bulk: fmt.Sprintf("%v", v)},
		)
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFCard handles the 'BF.CARD' command which returns the cardinality of the Bloom filter.
// It expects a single argument which is the key of the Bloom filter.
// If the number of arguments is incorrect, it returns an error.
// If the cardinality retrieval is successful, it returns the cardinality as an integer.
// If there is an error during the retrieval, it returns an error message.
//
// Args:
//
//	args ([]models.Value): A slice containing the arguments for the command.
//
// Returns:
//
//	models.Value: The result of the command execution, either the cardinality as an integer or an error message.
func (h *BloomFilterHandlers) HandleBFCard(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.CARD' command",
		}
	}

	card, err := h.cache.BFCard(args[0].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  int(card),
	}
}

// HandleBFScanDump handles the 'BF.SCANDUMP' command for the Bloom filter.
// It expects exactly two arguments: the key of the Bloom filter and the iterator value.
// If the number of arguments is incorrect, it returns an error.
// It converts the iterator argument to an integer and validates it.
// It then calls the BFScanDump method on the cache with the provided key and iterator.
// If the BFScanDump method returns an error, it returns an error.
// Otherwise, it returns the next iterator and the dumped data as an array.
//
// Args:
//
//	args ([]models.Value): The arguments for the 'BF.SCANDUMP' command.
//
// Returns:
//
//	models.Value: The result of the 'BF.SCANDUMP' command, either an error or an array
//	containing the next iterator and the dumped data.
func (h *BloomFilterHandlers) HandleBFScanDump(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.SCANDUMP' command",
		}
	}

	iterator, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid iterator value",
		}
	}

	nextIterator, data, err := h.cache.BFScanDump(args[0].Bulk, iterator)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "integer", Num: nextIterator},
			{Type: "bulk", Bulk: string(data)},
		},
	}
}

// HandleBFLoadChunk handles the 'BF.LOADCHUNK' command which loads a chunk of data into a Bloom filter.
// It expects three arguments: the key of the Bloom filter, the iterator value, and the chunk of data to load.
// If the number of arguments is incorrect, it returns an error.
// If the iterator value is invalid, it returns an error.
// If the chunk loading operation fails, it returns an error.
// On success, it returns an OK response.
//
// Args:
//
//	args ([]models.Value): A slice of Value objects representing the command arguments.
//
// Returns:
//
//	models.Value: A Value object representing the result of the command execution.
func (h *BloomFilterHandlers) HandleBFLoadChunk(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.LOADCHUNK' command",
		}
	}

	iterator, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid iterator value",
		}
	}

	err = h.cache.BFLoadChunk(args[0].Bulk, iterator, []byte(args[2].Bulk))
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "string",
		Str:  "OK",
	}
}
