package json

// MergeOptions holds configuration for merge operations
type MergeOptions struct {
	MaxDepth          int
	OverwriteExisting bool
	SkipNullValues    bool
}

// DefaultMergeOptions returns default merge options
func DefaultMergeOptions() *MergeOptions {
	return &MergeOptions{
		OverwriteExisting: true,
		MaxDepth:          -1,
		SkipNullValues:    false,
	}
}

// Merge provides JSON merging functionality
type Merge struct{}

// NewMerge creates a new instance of Merge
func NewMerge() *Merge {
	return &Merge{}
}

// DeepMerge performs a deep merge of source into target
func (m *Merge) DeepMerge(target, source map[string]interface{}) map[string]interface{} {
	return m.DeepMergeWithOptions(target, source, DefaultMergeOptions())
}

// DeepMergeWithOptions performs a deep merge with custom options
func (m *Merge) DeepMergeWithOptions(target, source map[string]interface{}, opts *MergeOptions) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy target map
	for k, v := range target {
		result[k] = v
	}

	// Process source map
	for k, v := range source {
		// Skip null values if configured
		if opts.SkipNullValues && v == nil {
			continue
		}

		// Check if key exists in target
		targetVal, targetExists := target[k]

		// Handle nested maps
		if targetExists {
			if targetMap, isTargetMap := targetVal.(map[string]interface{}); isTargetMap {
				if sourceMap, isSourceMap := v.(map[string]interface{}); isSourceMap {
					// Check depth limit
					if opts.MaxDepth != 0 {
						newOpts := *opts
						if newOpts.MaxDepth > 0 {
							newOpts.MaxDepth--
						}
						result[k] = m.DeepMergeWithOptions(targetMap, sourceMap, &newOpts)
						continue
					}
				}
			}

			// Handle arrays
			if targetArr, isTargetArr := targetVal.([]interface{}); isTargetArr {
				if sourceArr, isSourceArr := v.([]interface{}); isSourceArr {
					result[k] = m.mergeArrays(targetArr, sourceArr, opts)
					continue
				}
			}
		}

		// Handle overwrite behavior
		if opts.OverwriteExisting || !targetExists {
			result[k] = v
		}
	}

	return result
}

// mergeArrays combines two arrays based on merge options
func (m *Merge) mergeArrays(target, source []interface{}, opts *MergeOptions) []interface{} {
	if !opts.OverwriteExisting {
		return target
	}

	// Create new array with combined length
	result := make([]interface{}, len(target)+len(source))
	copy(result, target)
	copy(result[len(target):], source)

	return result
}

// MergeMultiple merges multiple JSON objects together
func (m *Merge) MergeMultiple(objects ...map[string]interface{}) map[string]interface{} {
	if len(objects) == 0 {
		return make(map[string]interface{})
	}

	result := objects[0]
	for i := 1; i < len(objects); i++ {
		result = m.DeepMerge(result, objects[i])
	}

	return result
}

// MergeMultipleWithOptions merges multiple JSON objects with custom options
func (m *Merge) MergeMultipleWithOptions(opts *MergeOptions, objects ...map[string]interface{}) map[string]interface{} {
	if len(objects) == 0 {
		return make(map[string]interface{})
	}

	result := objects[0]
	for i := 1; i < len(objects); i++ {
		result = m.DeepMergeWithOptions(result, objects[i], opts)
	}

	return result
}

// MergePatch applies a JSON merge patch according to RFC 7396
func (m *Merge) MergePatch(target, patch map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy target
	for k, v := range target {
		result[k] = v
	}

	// Apply patch
	for k, v := range patch {
		if v == nil {
			delete(result, k)
		} else if targetVal, exists := target[k]; exists {
			if targetMap, isTargetMap := targetVal.(map[string]interface{}); isTargetMap {
				if patchMap, isPatchMap := v.(map[string]interface{}); isPatchMap {
					result[k] = m.MergePatch(targetMap, patchMap)
					continue
				}
			}
			result[k] = v
		} else {
			result[k] = v
		}
	}

	return result
}
