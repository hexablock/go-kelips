package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	kelips "github.com/hexablock/go-kelips"
)

var (
	advAddr   = flag.String("adv-addr", "127.0.0.1:43210", "Advertise address")
	httpAddr  = flag.String("http-addr", "127.0.0.1:7070", "HTTP advertise address")
	joinAddrs = flag.String("join", "", "Existing peers to join")
)

func parseAdvAddr() (string, int64, error) {
	hp := strings.Split(*advAddr, ":")
	port, err := strconv.ParseInt(hp[1], 10, 32)
	return hp[0], port, err
}

func parsePeers() []string {
	if *joinAddrs == "" {
		return []string{}
	}
	return strings.Split(*joinAddrs, ",")
}

func initTransport() (*kelips.UDPTransport, error) {

	udpAddr, err := net.ResolveUDPAddr("udp", *advAddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return kelips.NewUDPTransport(conn), nil
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	log.SetPrefix("| " + *advAddr + " | ")

	conf := kelips.DefaultConfig(*advAddr)
	// We have no gossip transport so we enable propogation
	conf.EnablePropogation = true
	trans, err := initTransport()
	if err != nil {
		log.Fatal(err)
	}

	kelps := kelips.Create(conf, trans)
	log.Println("Started cluster on", *advAddr)

	log.Println("Starting HTTP on", *httpAddr)
	err = http.ListenAndServe(*httpAddr, &httpServer{klp: kelps})
	if err != nil {
		log.Fatal(err)
	}

}
