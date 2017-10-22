package kelips

import (
	"log"
	"time"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"

	"github.com/hashicorp/serf/serf"
)

const (
	// lookup request
	queryLookup = "lookup"
	// insert was made
	userInsert = "insert"
)

func (kelps *Kelips) handleEvents() {
	var err error
	for event := range kelps.events {

		typ := event.EventType()
		switch typ {

		case serf.EventQuery:
			evt := event.(*serf.Query)
			err = kelps.handleQuery(evt)

		case serf.EventUser:
			evt := event.(serf.UserEvent)
			err = kelps.handleUser(evt)

		case serf.EventMemberJoin:
			evt := event.(serf.MemberEvent)
			err = kelps.handleJoin(evt)

		case serf.EventMemberFailed, serf.EventMemberLeave:
			evt := event.(serf.MemberEvent)
			err = kelps.handleMemLeftOrFailed(evt)

		case serf.EventMemberReap, serf.EventMemberUpdate:
			evt := event.(serf.MemberEvent)
			log.Println("[DEBUG]", typ, event, evt.Members)
		}

		if err != nil {
			log.Printf("[ERROR] event=%s error=%v", typ, err)
		}

	}

	kelps.stopped <- struct{}{}
}

func (kelps *Kelips) newNode(mem serf.Member) *Node {
	nd := &Node{
		Name:       mem.Name,
		Tags:       mem.Tags,
		Host:       &Host{mem.Addr, mem.Port},
		LastSeen:   time.Now(),
		Heartbeats: 1,
	}
	nd.init(kelps.conf.HashFunc)
	return nd
}

// handleJoin adds the nodes to their associated affinity groups.  It
// returns the last encountered error
func (kelps *Kelips) handleJoin(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {

		node := kelps.newNode(mem)
		group := kelps.groups.get(node.ID)
		if er := group.addNode(node); err != nil {
			err = er
		}

	}
	return
}

// handleMemLeftOrFailed removes the failed node from its associated AffinityGroup.
// It returns the last encountered error
func (kelps *Kelips) handleMemLeftOrFailed(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {

		node := kelps.newNode(mem)
		group := kelps.groups.get(node.ID)
		if er := group.removeNode(node.String()); er != nil {
			err = er
		}

	}

	return
}

func (kelps *Kelips) handleUser(evt serf.UserEvent) error {
	typ := evt.Name
	switch typ {

	case userInsert:
		data := evt.Payload
		pos := len(data) - 18

		key := data[:pos]
		group := kelps.LookupGroup(key)
		if group.Index() == kelps.idx {
			host := NewHostFromBytes(data[pos:])
			kelps.tuples.Add(string(key), host)
		}

	}

	return nil
}

func (kelps *Kelips) handleQuery(query *serf.Query) error {
	typ := query.Name
	switch typ {

	case queryLookup:
		group := kelps.LookupGroup(query.Payload)
		// Only process if we are in the group
		if group.Index() != kelps.idx {
			break
		}

		nodes := kelps.getTupleNodes(group, query.Payload)
		buf, err := msgpack.Marshal(nodes)
		if err != nil {
			return err
		}

		return query.Respond(buf)
	}
	return nil
}
