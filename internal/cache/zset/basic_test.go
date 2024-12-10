package zset

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZAddAndZCard(t *testing.T) {
	cache := &sync.Map{}
	keyVersions := &sync.Map{}
	ops := NewBasicOps(cache, keyVersions)

	t.Run("ZAdd", func(t *testing.T) {
		err := ops.ZAdd("myzset", 1.0, "member1")
		assert.NoError(t, err)

		err = ops.ZAdd("myzset", 2.0, "member2")
		assert.NoError(t, err)
	})

	t.Run("ZCard", func(t *testing.T) {
		card := ops.ZCard("myzset")
		assert.Equal(t, 2, card)
	})
}

func TestZScore(t *testing.T) {
	cache := &sync.Map{}
	keyVersions := &sync.Map{}
	ops := NewBasicOps(cache, keyVersions)

	t.Run("AddMembers", func(t *testing.T) {
		ops.ZAdd("myzset", 1.0, "member1")
		ops.ZAdd("myzset", 2.0, "member2")
	})

	t.Run("ZScoreExists", func(t *testing.T) {
		score, exists := ops.ZScore("myzset", "member1")
		assert.True(t, exists)
		assert.Equal(t, 1.0, score)
	})

	t.Run("ZScoreNonExistent", func(t *testing.T) {
		_, exists := ops.ZScore("myzset", "nonexistent")
		assert.False(t, exists)
	})
}

func TestZRem(t *testing.T) {
	cache := &sync.Map{}
	keyVersions := &sync.Map{}
	ops := NewBasicOps(cache, keyVersions)

	t.Run("AddMembers", func(t *testing.T) {
		ops.ZAdd("myzset", 1.0, "member1")
		ops.ZAdd("myzset", 2.0, "member2")
	})

	t.Run("RemoveMember", func(t *testing.T) {
		err := ops.ZRem("myzset", "member1")
		assert.NoError(t, err)
	})

	t.Run("CheckExistence", func(t *testing.T) {
		score, exists := ops.ZScore("myzset", "member1")
		assert.False(t, exists)
		assert.Equal(t, 0.0, score)
	})

	t.Run("ZCardAfterRemove", func(t *testing.T) {
		card := ops.ZCard("myzset")
		assert.Equal(t, 1, card)
	})
}

func TestZRandMember(t *testing.T) {
	cache := &sync.Map{}
	keyVersions := &sync.Map{}
	ops := NewBasicOps(cache, keyVersions)

	t.Run("AddMembers", func(t *testing.T) {
		ops.ZAdd("myzset", 1.0, "member1")
		ops.ZAdd("myzset", 2.0, "member2")
		ops.ZAdd("myzset", 3.0, "member3")
	})

	t.Run("GetRandomMembers", func(t *testing.T) {
		members := ops.ZRandMember("myzset", 2, true)
		assert.Len(t, members, 2)

		for _, member := range members {
			assert.Contains(t, []string{"member1", "member2", "member3"}, member.Member)
		}
	})
}

func TestZRandMemberWithoutScores(t *testing.T) {
	cache := &sync.Map{}
	keyVersions := &sync.Map{}
	ops := NewBasicOps(cache, keyVersions)

	t.Run("AddMembers", func(t *testing.T) {
		ops.ZAdd("myzset", 1.0, "member1")
		ops.ZAdd("myzset", 2.0, "member2")
		ops.ZAdd("myzset", 3.0, "member3")
	})

	t.Run("GetRandomMembersWithoutScores", func(t *testing.T) {
		members := ops.ZRandMemberWithoutScores("myzset", 2)
		assert.Len(t, members, 2)

		for _, member := range members {
			assert.Contains(t, []string{"member1", "member2", "member3"}, member)
		}
	})
}
