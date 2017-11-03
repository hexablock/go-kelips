package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	kelips "github.com/hexablock/kelips"
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

func initConf() *kelips.Config {

	conf := kelips.DefaultConfig(*advAddr)
	conf.Hostname = *advAddr

	return conf
}

func parsePeers() []string {
	if *joinAddrs == "" {
		return []string{}
	}
	return strings.Split(*joinAddrs, ",")
}

type httpServer struct {
	klp *kelips.Kelips
}

func (hs *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqpath := r.URL.Path[1:]
	if reqpath == "" {
		w.WriteHeader(404)
		return
	}

	// if strings.HasPrefix(reqpath, "group/") {
	// 	hs.handleGroup(w, strings.TrimPrefix(reqpath, "group/"))
	// 	return
	// }

	var (
		err error
	)

	switch r.Method {
	case "GET":
		nodes, er := hs.klp.Lookup([]byte(reqpath))
		if er != nil {
			err = er
			break
		}

		b, er := json.Marshal(nodes)
		if er != nil {
			err = er
			break
		}
		w.Write(b)

	case "POST":
		hpath := strings.Split(reqpath, "/")
		if len(hpath) != 2 {
			err = fmt.Errorf("must be in format {host}:{ip}/{key}")
			break
		}

		tuple := kelips.NewTupleHost(hpath[0])
		err = hs.klp.Insert([]byte(hpath[1]), tuple)

	default:
		w.WriteHeader(405)
		return
	}

	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error": "` + err.Error() + `"}`))
	}

}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	log.SetPrefix("| " + *advAddr + " | ")

	conf := initConf()

	udpAddr, err := net.ResolveUDPAddr("udp", *advAddr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal(err)
	}

	trans := kelips.NewUDPTransport(conn)

	//	coords, _ := vivaldi.NewClient(vivaldi.DefaultConfig())
	// if err != nil {
	// 	log.Fatal(err)
	// }
	kelps := kelips.Create(conf, trans)

	log.Println("Started cluster on", *advAddr)

	// peers := parsePeers()
	// if len(peers) > 0 {
	// 	log.Println("Joining", *joinAddrs)
	// 	if err = trans.Join(peers...); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	handler := &httpServer{klp: kelps}
	log.Println("Starting HTTP on", *httpAddr)
	if err = http.ListenAndServe(*httpAddr, handler); err != nil {
		log.Fatal(err)
	}

}
