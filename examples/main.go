package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/hashicorp/serf/serf"
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

func initConf() (*kelips.Config, *serf.Config) {
	host, port, err := parseAdvAddr()
	if err != nil {
		log.Fatal(err)
	}

	conf := kelips.DefaultConfig()

	serfConf := serf.DefaultConfig()
	serfConf.NodeName = *advAddr
	serfConf.Tags = map[string]string{"key": "value"}

	s := serfConf.MemberlistConfig

	s.AdvertiseAddr = host
	s.BindAddr = host
	s.AdvertisePort = int(port)
	s.BindPort = int(port)

	return conf, serfConf
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

	if strings.HasPrefix(reqpath, "group/") {
		hs.handleGroup(w, strings.TrimPrefix(reqpath, "group/"))
		return
	}

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
		hp := strings.Split(hpath[0], ":")
		var port int64
		if port, err = strconv.ParseInt(hp[1], 10, 64); err == nil {
			host := kelips.NewHost(hp[0], uint16(port))
			err = hs.klp.Insert([]byte(hpath[1]), host)
		}

	default:
		w.WriteHeader(405)
		return
	}

	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(`{"error": "` + err.Error() + `"}`))
	}

}

func (hs *httpServer) handleGroup(w http.ResponseWriter, reqpath string) {
	if reqpath == "" {
		w.WriteHeader(404)
		return
	}

	nodes := hs.klp.LookupGroup([]byte(reqpath))
	b, err := json.Marshal(nodes)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	} else {
		w.Write(b)
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	log.SetPrefix("| " + *advAddr + " | ")

	conf, serfConf := initConf()
	serfConf.LogOutput = ioutil.Discard
	serfConf.MemberlistConfig.LogOutput = ioutil.Discard

	kelps, err := kelips.NewKelips(conf, serfConf)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Started", *advAddr)

	peers := parsePeers()
	if len(peers) > 0 {
		log.Println("Joining", *joinAddrs)
		if err = kelps.Join(peers...); err != nil {
			log.Fatal(err)
		}
	}

	handler := &httpServer{klp: kelps}
	if err = http.ListenAndServe(*httpAddr, handler); err != nil {
		log.Fatal(err)
	}

}
