package experiments

import (
	"math/rand/v2"
	"sort"
)

// Level is what the book keeps per price: aggregate quantity and the ends of
// the order queue (as in internal/engine).
type Level struct {
	Price      int64
	Qty        int64
	Head, Tail int32
}

// sliceSide is the engine's structure: levels sorted by price in a slice, best
// price at the end, binary search to find a level, copy to insert in the
// middle.
type sliceSide struct{ lv []Level }

func (s *sliceSide) findOrInsert(p int64) *Level {
	i := sort.Search(len(s.lv), func(i int) bool { return s.lv[i].Price >= p })
	if i < len(s.lv) && s.lv[i].Price == p {
		return &s.lv[i]
	}
	s.lv = append(s.lv, Level{})
	copy(s.lv[i+1:], s.lv[i:])
	s.lv[i] = Level{Price: p, Head: -1, Tail: -1}
	return &s.lv[i]
}

func (s *sliceSide) remove(p int64) {
	i := sort.Search(len(s.lv), func(i int) bool { return s.lv[i].Price >= p })
	if i < len(s.lv) && s.lv[i].Price == p {
		copy(s.lv[i:], s.lv[i+1:])
		s.lv = s.lv[:len(s.lv)-1]
	}
}

// skipList is a plain probabilistic skip list keyed by price.
type skipList struct {
	head  *skipNode
	level int
	rng   *rand.Rand
}

const maxSkip = 20

type skipNode struct {
	lv   Level
	next [maxSkip]*skipNode
}

func newSkipList() *skipList {
	return &skipList{head: &skipNode{}, level: 1, rng: rand.New(rand.NewPCG(1, 2))}
}

func (s *skipList) randomLevel() int {
	l := 1
	for l < maxSkip && s.rng.Uint32()&3 == 0 { // p = 1/4
		l++
	}
	return l
}

func (s *skipList) findOrInsert(p int64) *Level {
	var update [maxSkip]*skipNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].lv.Price < p {
			x = x.next[i]
		}
		update[i] = x
	}
	if n := x.next[0]; n != nil && n.lv.Price == p {
		return &n.lv
	}
	l := s.randomLevel()
	for i := s.level; i < l; i++ {
		update[i] = s.head
	}
	if l > s.level {
		s.level = l
	}
	n := &skipNode{lv: Level{Price: p, Head: -1, Tail: -1}}
	for i := 0; i < l; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	return &n.lv
}

func (s *skipList) remove(p int64) {
	var update [maxSkip]*skipNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].lv.Price < p {
			x = x.next[i]
		}
		update[i] = x
	}
	n := x.next[0]
	if n == nil || n.lv.Price != p {
		return
	}
	for i := 0; i < s.level; i++ {
		if update[i].next[i] != n {
			break
		}
		update[i].next[i] = n.next[i]
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
}
