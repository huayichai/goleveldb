package version

import "github.com/huayichai/goleveldb/internal"

type Compaction struct {
	level  int
	inputs [2][]*FileMetaData
}

func (c *Compaction) Level() int {
	return c.level
}

func (c *Compaction) Input() [2][]*FileMetaData {
	return c.inputs
}

// Is this a trivial compaction that can be implemented by just
// moving a single input file to the next level (no merging or splitting)
func (c *Compaction) IsTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

func (v *Version) PickCompaction() *Compaction {
	var c Compaction
	c.level, _ = v.pickCompactionLevel()
	if c.level < 0 {
		return nil
	}

	// Pick the first file that comes after compact_pointer_[level]
	for i := 0; i < len(v.Files[c.level]); i++ {
		f := v.Files[c.level][i]
		if v.compactPointer[c.level] == nil || internal.InternalKeyCompare(f.Largest, v.compactPointer[c.level]) > 0 {
			c.inputs[0] = append(c.inputs[0], f)
			break
		}
	}
	if len(c.inputs[0]) == 0 {
		c.inputs[0] = append(c.inputs[0], v.Files[c.level][0])
	}

	// Files in level 0 may overlap each other, so pick up all overlapping ones
	if c.level == 0 {
		smallest, largest := v.getRange(c.inputs[0])
		c.inputs[0] = v.getOverlappingInputs(0, smallest, largest)
	}

	v.setupOtherInputs(&c)

	return &c
}

func (v *Version) pickCompactionLevel() (int, float64) {
	best_level := -1
	best_score := -1.0
	for level := 0; level < int(internal.NumLevels-1); level++ {
		var score float64
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compactions.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(v.Files[level])) / float64(internal.L0_CompactionTrigger)
		} else {
			score = float64(totalFileSize(v.Files[level])) / maxBytesForLevel(level)
		}
		if score > best_score {
			best_level = level
			best_score = score
		}
	}
	return best_level, best_score
}

// Stores the minimal range that covers all entries in inputs in
// @return smallest, largest.
func (v *Version) getRange(metas []*FileMetaData) (internal.InternalKey, internal.InternalKey) {
	var smallest, largest internal.InternalKey
	for i := 0; i < len(metas); i++ {
		if i == 0 {
			smallest = metas[i].Smallest
			largest = metas[i].Largest
		} else {
			if internal.InternalKeyCompare(metas[i].Smallest, smallest) < 0 {
				smallest = metas[i].Smallest
			}
			if internal.InternalKeyCompare(metas[i].Largest, largest) > 0 {
				largest = metas[i].Largest
			}
		}
	}
	return smallest, largest
}

// Store in "outputs" all files in "level" that overlap [begin,end]
func (v *Version) getOverlappingInputs(level int, begin, end internal.InternalKey) []*FileMetaData {
	user_begin, user_end := begin.ExtractUserKey(), end.ExtractUserKey()
	outputs := make([]*FileMetaData, 0)
	for i := 0; i < len(v.Files[level]); i++ {
		f := v.Files[level][i]
		file_start := f.Smallest.ExtractUserKey()
		file_limit := f.Largest.ExtractUserKey()
		if internal.UserKeyCompare(file_limit, user_begin) < 0 {
			// "f" is completely before specified range; skip it
		} else if internal.UserKeyCompare(file_start, user_end) > 0 {
			// "f" is completely after specified range; skip it
		} else {
			outputs = append(outputs, f)
			if level == 0 {
				// Level-0 files may overlap each other.  So check if the newly
				// added file has expanded the range.  If so, restart search.
				if internal.UserKeyCompare(file_start, user_begin) < 0 {
					user_begin = file_start
					outputs = outputs[0:0]
					i = 0
				} else if internal.UserKeyCompare(file_limit, user_end) > 0 {
					user_end = file_limit
					outputs = outputs[0:0]
					i = 0
				}
			}
		}
	}
	return outputs
}

func (v *Version) setupOtherInputs(c *Compaction) {
	smallest, largest := v.getRange(c.inputs[0])
	c.inputs[1] = v.getOverlappingInputs(c.level+1, smallest, largest)
	v.compactPointer[c.level] = largest
}

func totalFileSize(files []*FileMetaData) uint64 {
	var sum uint64
	sum = 0
	for i := 0; i < len(files); i++ {
		sum += files[i].FileSize
	}
	return sum
}

func maxBytesForLevel(level int) float64 {
	// Note: the result for level zero is not really used since we set
	// the level-0 compaction threshold based on number of files.

	// Result for both level-0 and level-1
	result := 10 * 1048576.0
	for level > 1 {
		result *= 10
		level--
	}
	return result
}
