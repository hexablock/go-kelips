package kelips

import (
	"sync"
)

type filetuples struct {
	mu sync.RWMutex
	m  map[string][]*Host
}

func newFiletuples() *filetuples {
	return &filetuples{m: make(map[string][]*Host)}
}

func (ft *filetuples) add(name string, h *Host) {
	hosts, ok := ft.m[name]
	if !ok {
		ft.m[name] = []*Host{h}
		return
	}

	ok = false
	for _, v := range hosts {
		if h.Hostname() == v.Hostname() {
			ok = true
			break
		}
	}
	if !ok {
		ft.m[name] = append(hosts, h)
	}
}

func (ft *filetuples) del(name string, h *Host) {
	ft.mu.RLock()
	hosts, ok := ft.m[name]
	if !ok {
		ft.mu.RUnlock()
		return
	}
	l := len(hosts)

	for i, v := range hosts {
		if h.Hostname() == v.Hostname() {
			ft.mu.RUnlock()

			ft.mu.Lock()
			if i == 0 {
				ft.m[name] = hosts[1:]
			} else if i == l {
				ft.m[name] = hosts[:l-1]
			} else {
				ft.m[name] = append(hosts[:i], hosts[i+1:]...)
			}
			ft.mu.Unlock()
			return
		}
	}
	ft.mu.RUnlock()
}

func (ft *filetuples) get(name string) []*Host {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	hosts, _ := ft.m[name]
	return hosts
}
