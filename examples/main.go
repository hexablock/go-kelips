package main

import (
	"encoding/json"
	"flag"
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

	switch r.Method {
	case "GET":
		nodes := hs.klp.LookupGroup([]byte(reqpath))
		b, err := json.Marshal(nodes)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
		} else {
			w.Write(b)
		}
	case "POST":
		_, ok := hs.klp.Insert([]byte(reqpath))
		if !ok {
			// other group
		}

	default:
		w.WriteHeader(405)
		return
	}

}

func main() {
	flag.Parse()

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
		if err = kelps.Join(peers); err != nil {
			log.Fatal(err)
		}
	}

	handler := &httpServer{klp: kelps}
	if err = http.ListenAndServe(*httpAddr, handler); err != nil {
		log.Fatal(err)
	}

	// srf := kelps.Serf()
	// for {
	// 	time.Sleep(15 * time.Second)
	// 	log.Println(srf.GetCachedCoordinate(*advAddr))
	// }

	// sig := make(chan os.Signal, 1)
	// signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	// <-sig
	// kelps.Shutdown()
}
