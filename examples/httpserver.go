package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	kelips "github.com/hexablock/go-kelips"
)

type httpServer struct {
	klp *kelips.Kelips
}

func (hs *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqpath := r.URL.Path[1:]
	if reqpath == "" {
		w.WriteHeader(404)
		return
	}

	var (
		err  error
		code int
	)

	switch r.Method {
	case "GET":
		nodes, er := hs.klp.Lookup([]byte(reqpath))
		if er != nil {
			err = er
			code = 404
			break
		}

		b, er := json.Marshal(nodes)
		if er != nil {
			err = er
			code = 500
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

	if err == nil {
		return
	}

	if code < 400 {
		code = 400
	}

	w.WriteHeader(code)
	w.Write([]byte(`{"error": "` + err.Error() + `"}`))
}
