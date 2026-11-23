package sdk

// Position keeps outstanding IDs until every earlier message is acknowledged.
type Position struct {
	order []string
	acked map[string]bool
}

func (p *Position) Sent(id string) {
	if p.acked == nil {
		p.acked = make(map[string]bool)
	}
	if _, exists := p.acked[id]; !exists {
		p.order = append(p.order, id)
		p.acked[id] = false
	}
}

func (p *Position) Ack(id string) []string {
	if acked, ok := p.acked[id]; !ok || acked {
		return nil
	}
	p.acked[id] = true
	n := 0
	for n < len(p.order) && p.acked[p.order[n]] {
		delete(p.acked, p.order[n])
		n++
	}
	ready := p.order[:n:n]
	p.order = p.order[n:]
	if len(p.order) == 0 {
		p.order = nil
	}
	return ready
}
