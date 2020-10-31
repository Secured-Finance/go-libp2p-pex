package pex

type stack struct {
	items []interface{}
}

func newStack() *stack {
	return &stack{
		items: []interface{}{},
	}
}

func (s *stack) Len() int {
	return len(s.items)
}

func (s *stack) Peek() interface{} {
	if len(s.items) == 0 {
		return nil
	}
	return s.items[len(s.items)-1]
}

func (s *stack) Pop() interface{} {
	if len(s.items) == 0 {
		return nil
	}

	top := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return top
}

func (s *stack) Push(item interface{}) {
	s.items = append(s.items, item)
}

func (s *stack) Shuffle() {
	Random.Shuffle(len(s.items), func(i, j int) { s.items[i], s.items[j] = s.items[j], s.items[i] })
}
