package kelips

import (
	"fmt"
	"net"

	"github.com/golang/protobuf/proto"

	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

const (
	reqTypeLookup byte = iota + 3
	reqTypeLookupGroupNodes
	reqTypeInsert
	reqTypeDelete
)

const (
	respTypeOk byte = iota + 10
	respTypeFail
)

const maxBufSize = 1256

// UDPTransport is a udp based transport for kelips.  It is well suited due to
// the small message size and reliance on gossip.  It only implements rpc's
// and not the fault-tolerance.  This is primarily used for direct inserts,
// lookups and deletes
type UDPTransport struct {
	conn *net.UDPConn

	local AffinityGroupRPC
}

// NewUDPTransport inits a new UDPTransport using the given server connection.
// Nil can be supplied if the transport is only used as a client and is not a
// cluster member
func NewUDPTransport(ln *net.UDPConn) *UDPTransport {
	return &UDPTransport{conn: ln}
}

// Lookup performs a lookup request on a host for a key
func (trans *UDPTransport) Lookup(host string, key []byte) ([]*hexatype.Node, error) {
	conn, err := trans.getConn(host)
	if err != nil {
		return nil, err
	}

	req := append([]byte{reqTypeLookup}, key...)
	if _, err = conn.Write(req); err != nil {
		return nil, err
	}

	buf, err := trans.readResponse(conn)
	if err != nil {
		return nil, err
	}

	var rr ReqResp
	if err = proto.Unmarshal(buf, &rr); err == nil {
		return rr.Nodes, nil
	}

	return nil, err
}

// LookupGroupNodes looksup the group nodes for a key on a remote host
func (trans *UDPTransport) LookupGroupNodes(host string, key []byte) ([]*hexatype.Node, error) {
	conn, err := trans.getConn(host)
	if err != nil {
		return nil, err
	}

	req := append([]byte{reqTypeLookupGroupNodes}, key...)
	if _, err = conn.Write(req); err != nil {
		return nil, err
	}

	buf, err := trans.readResponse(conn)
	if err != nil {
		return nil, err
	}

	var rr ReqResp
	if err = proto.Unmarshal(buf, &rr); err == nil {
		return rr.Nodes, nil
	}

	return nil, err
}

// Insert inserts a key to node mapping on a remote host
func (trans *UDPTransport) Insert(host string, key []byte, tuple TupleHost, propogate bool) error {

	conn, err := trans.getConn(host)
	if err != nil {
		return err
	}

	data := append(tuple, key...)
	var req []byte
	if propogate {
		req = append([]byte{reqTypeInsert, byte(1)}, data...)
	} else {
		req = append([]byte{reqTypeInsert, byte(0)}, data...)
	}

	if _, err = conn.Write(req); err != nil {
		return err
	}

	_, err = trans.readResponse(conn)
	return err
}

// Delete a key on the the host removing all node mappings for the key
func (trans *UDPTransport) Delete(host string, key []byte, propogate bool) error {
	conn, err := trans.getConn(host)
	if err != nil {
		return err
	}

	var req []byte
	if propogate {
		req = append([]byte{reqTypeDelete, byte(1)}, key...)
	} else {
		req = append([]byte{reqTypeDelete, byte(0)}, key...)
	}

	if _, err = conn.Write(req); err != nil {
		return err
	}

	_, err = trans.readResponse(conn)
	return err
}

// Register registers the local group to serve rpcs from and starts accepting
// connections
func (trans *UDPTransport) Register(group AffinityGroupRPC) {
	trans.local = group
	log.Println("[INFO] DHT serving on:", trans.conn.LocalAddr())
	go trans.listen()
}

func (trans *UDPTransport) handleRequest(remote *net.UDPAddr, typ byte, msg []byte) {
	var (
		err  error
		resp []byte
	)

	switch typ {

	case reqTypeLookup:
		rr := &ReqResp{}
		if rr.Nodes, err = trans.local.Lookup(msg); err != nil {
			break
		}

		if len(rr.Nodes) == 0 {
			err = fmt.Errorf("no nodes found")
			break
		}

		resp, err = proto.Marshal(rr)

	case reqTypeLookupGroupNodes:
		rr := &ReqResp{}
		if rr.Nodes, err = trans.local.LookupGroupNodes(msg); err != nil {
			break
		}

		if len(rr.Nodes) == 0 {
			err = fmt.Errorf("no nodes found")
			break
		}

		resp, err = proto.Marshal(rr)

	case reqTypeInsert:
		if len(msg) < 20 {
			err = fmt.Errorf("insert size too small")
			break
		}
		prop := msg[0]
		tuple := TupleHost(msg[1:19])
		key := msg[19:]

		if prop == byte(1) {
			err = trans.local.Insert(key, tuple, true)
		} else {
			err = trans.local.Insert(key, tuple, false)
		}

	case reqTypeDelete:
		prop := msg[0]
		if prop == byte(1) {
			err = trans.local.Delete(msg[1:], true)
		} else {
			err = trans.local.Delete(msg[1:], false)
		}

	default:
		err = fmt.Errorf("unknown request: %x '%s'", typ, msg)
	}

	if err != nil {
		resp = append([]byte{respTypeFail}, []byte(err.Error())...)
	} else {
		if resp != nil {
			resp = append([]byte{respTypeOk}, resp...)
		} else {
			resp = []byte{respTypeOk}
		}
	}

	var w int
	w, err = trans.conn.WriteToUDP(resp, remote)
	if err != nil {
		log.Println("[ERROR] Failed to write response:", err)
	} else {
		if w != len(resp) {
			log.Println("[ERROR] Incomplete response write", w, len(resp))
		}
	}

}

func (trans *UDPTransport) listen() {
	buf := make([]byte, maxBufSize)

	for {
		n, remote, err := trans.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("[ERROR]", err)
			continue
		}

		typ := buf[0]
		msg := make([]byte, n-1)
		copy(msg, buf[1:n])

		go trans.handleRequest(remote, typ, msg)
	}
}

func (trans *UDPTransport) getConn(host string) (*net.UDPConn, error) {
	raddr, err := net.ResolveUDPAddr("udp4", host)
	if err == nil {
		return net.DialUDP("udp4", nil, raddr)
	}
	return nil, err
}

func (trans *UDPTransport) readResponse(conn *net.UDPConn) ([]byte, error) {
	buf := make([]byte, maxBufSize)
	n, err := conn.Read(buf)
	if err == nil {
		b := buf[:n]

		if b[0] == respTypeOk {
			return b[1:], nil
		}
		err = fmt.Errorf("%s", b[1:])
	}

	return nil, err
}
