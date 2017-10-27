package kelips

import (
	"fmt"
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

func (trans *SerfTransport) handleEvents() {
	var err error

	for {
		select {

		case event := <-trans.events:
			typ := event.EventType()
			switch typ {

			case serf.EventQuery:
				evt := event.(*serf.Query)
				err = trans.handleQuery(evt)

			case serf.EventUser:
				evt := event.(serf.UserEvent)
				err = trans.handleUser(evt)

			case serf.EventMemberJoin:
				evt := event.(serf.MemberEvent)
				err = trans.handleJoin(evt)

			case serf.EventMemberFailed, serf.EventMemberLeave:
				evt := event.(serf.MemberEvent)
				err = trans.handleMemLeftOrFailed(evt)

			case serf.EventMemberReap, serf.EventMemberUpdate:
				evt := event.(serf.MemberEvent)
				log.Println("[DEBUG]", typ, event, evt.Members)

			}

			if err != nil {
				log.Printf("[ERROR] event=%s error=%v", typ, err)
			}

		case <-trans.shutdown:
			return
		}
	}

}

// handleJoin adds the nodes to their associated affinity groups.  It
// returns the last encountered error
func (trans *SerfTransport) handleJoin(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {

		node := &Node{
			Name:       mem.Name,
			Tags:       mem.Tags,
			Host:       &Host{mem.Addr, mem.Port},
			LastSeen:   time.Now(),
			Heartbeats: 1,
		}

		if er := trans.local.AddNode(node); er != nil {
			err = er
		}

	}
	return
}

// handleMemLeftOrFailed removes the failed node from its associated AffinityGroup.
// It returns the last encountered error
func (trans *SerfTransport) handleMemLeftOrFailed(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {

		host := fmt.Sprintf("%s:%d", mem.Addr, mem.Port)
		if er := trans.local.RemoveNode(host); er != nil {
			err = er
		}

	}

	return
}

func (trans *SerfTransport) handleUser(evt serf.UserEvent) error {
	data := evt.Payload
	typ := evt.Name
	switch typ {

	case userInsert:
		pos := len(data) - 18

		key := data[:pos]
		host := NewHostFromBytes(data[pos:])

		trans.local.AddTuple(string(key), host)
	}

	return nil
}

func (trans *SerfTransport) handleQuery(query *serf.Query) (err error) {
	typ := query.Name

	switch typ {

	case queryLookup:
		nodes := trans.local.GetTuples(query.Payload)
		var buf []byte
		if buf, err = msgpack.Marshal(nodes); err == nil {
			err = query.Respond(buf)
		}

	}

	return err
}

// Shutdown signals a shutdown of the even channel
func (trans *SerfTransport) Shutdown() {
	trans.shutdown <- struct{}{}
}
