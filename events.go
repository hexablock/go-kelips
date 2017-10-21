package kelips

import (
	"log"
	"time"

	"github.com/hashicorp/serf/serf"
)

func (kelps *Kelips) initNode(mem serf.Member) *Node {
	nd := &Node{
		Name:       mem.Name,
		Tags:       mem.Tags,
		Status:     int(mem.Status),
		Host:       &Host{mem.Addr, mem.Port},
		Heartbeats: 1,
	}
	nd.init(kelps.conf.HashFunc)
	return nd
}

// handleJoinEvent adds the nodes to their associated affinity groups.  It
// returns the last encountered error
func (kelps *Kelips) handleJoinEvent(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {
		nd := kelps.initNode(mem)
		nd.LastSeen = time.Now()

		group := kelps.groups.get(nd.ID)
		if er := group.addNode(nd); err != nil {
			err = er
		}
	}
	return
}

// handleMemFailed removes the failed node from its associated AffinityGroup.
// It returns the last encountered error
func (kelps *Kelips) handleMemLeft(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {
		nd := kelps.initNode(mem)
		//log.Println(nd.Status)
		group := kelps.groups.get(nd.ID)
		if er := group.removeNode(nd.Hostname()); er != nil {
			err = er
		}
	}
	//
	// TODO: reconcile
	//
	return
}

func (kelps *Kelips) handleMemFailed(event serf.MemberEvent) (err error) {
	for _, mem := range event.Members {
		nd := kelps.initNode(mem)
		group := kelps.groups.get(nd.ID)
		if er := group.removeNode(nd.Hostname()); er != nil {
			err = er
		}
	}
	//
	// TODO: reconcile
	//
	return
}

func (kelps *Kelips) handleUserEvent(evt serf.UserEvent) error {
	log.Println(evt)
	log.Printf("USER: %s", evt.Payload)
	return nil
}

func (kelps *Kelips) handleQuery(query *serf.Query) error {
	log.Println(query)
	return nil
}

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
			err = kelps.handleUserEvent(evt)

		case serf.EventMemberJoin:
			evt := event.(serf.MemberEvent)
			err = kelps.handleJoinEvent(evt)

		case serf.EventMemberFailed:
			evt := event.(serf.MemberEvent)
			err = kelps.handleMemFailed(evt)

		case serf.EventMemberLeave:
			evt := event.(serf.MemberEvent)
			err = kelps.handleMemLeft(evt)

		case serf.EventMemberReap, serf.EventMemberUpdate:
			evt := event.(serf.MemberEvent)
			log.Println(typ, event, evt.Members)
		}

		if err != nil {
			log.Printf("[ERROR] event=%s error=%v", typ, err)
		}

	}

	kelps.stopped <- struct{}{}
}
