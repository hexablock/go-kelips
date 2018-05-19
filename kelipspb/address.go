package kelipspb

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/hexablock/iputil"
)

// Address holds a ip:port format address.  IPv4 and IPv6 are supported
type Address []byte

func NewAddress(hostport string) Address {
	addr, port, _ := iputil.SplitHostPort(hostport)
	return newAddress(addr, port)
}

// NewAddress returns a new address type given the ip string and port number
func newAddress(addr string, port int) Address {
	ip := net.ParseIP(addr)
	pb := make([]byte, 2)
	binary.BigEndian.PutUint16(pb, uint16(port))
	return Address(append(ip, pb...))
}

// Port returns the port of the address
func (addr Address) Port() uint16 {
	return binary.BigEndian.Uint16(addr[len(addr)-2:])
}

// IP returns the ip of the address
func (addr Address) IP() net.IP {
	return net.IP(addr[:len(addr)-2])
}

func (addr Address) String() string {
	return addr.IP().String() + fmt.Sprintf(":%d", addr.Port())
}
