package kelips

import (
	"fmt"
	"testing"
)

func Test_filetuples(t *testing.T) {
	ft := newFiletuples()

	for i := 0; i < 10; i++ {
		ft.add(fmt.Sprintf("filetuple%d", i), NewHost("127.0.0.1", 1234+uint16(i)))
	}

	for i := 0; i < 5; i++ {
		ft.add(fmt.Sprintf("filetuple%d", i), NewHost("127.0.0.1", 21234))
	}
	ft.add("filetuple0", NewHost("127.0.0.1", 21234))

	for i := 0; i < 10; i++ {
		hosts := ft.get(fmt.Sprintf("filetuple%d", i))
		if hosts == nil || len(hosts) == 0 {
			t.Errorf("not found filetuple%d", i)
		}
	}

	for i := 0; i < 5; i++ {
		hosts := ft.get(fmt.Sprintf("filetuple%d", i))
		if len(hosts) != 2 {
			t.Fatal("should have 2 hosts")
		}
	}

	for i := 0; i < 5; i++ {
		ft.del(fmt.Sprintf("filetuple%d", i), NewHost("127.0.0.1", 21234))
	}

	for i := 0; i < 10; i++ {
		hosts := ft.get(fmt.Sprintf("filetuple%d", i))
		if len(hosts) != 1 {
			t.Errorf("should have=1 got=%d", len(hosts))
		}
	}

}
