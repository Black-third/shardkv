package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// writeSelfSignedCert generates a fresh certificate and key for 127.0.0.1 and writes
// them into the test's temporary directory, returning their paths.
//
// The material is generated per run rather than committed: a key in a repository is a
// key that has leaked, and one with a fixed expiry is a test that fails on a date
// nobody chose. It is its own CA too, so the same file serves as the root a replica
// verifies the master against.
func writeSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "shardkv-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		// The IP SAN is what makes verification succeed for a replica dialing
		// 127.0.0.1: a certificate with no matching name would be rejected, correctly.
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	write := func(path, blockType string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	write(certPath, "CERTIFICATE", der)
	write(keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

// startTLSServer starts a server whose listener is wrapped in TLS.
func startTLSServer(t *testing.T, opts TLSOptions) (*Server, string, func()) {
	t.Helper()
	s := New(store.New(8))
	if err := s.EnableTLS(opts); err != nil {
		t.Fatalf("EnableTLS: %v", err)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	return s, s.Addr().String(), func() {
		cancel()
		<-done
	}
}

// TestTLSListener covers the client-facing half: a TLS client is served normally, and
// a plain-TCP client gets nowhere, which is the whole point of turning it on.
func TestTLSListener(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	opts := TLSOptions{CertFile: certPath, KeyFile: keyPath, CAFile: certPath}
	_, addr, stop := startTLSServer(t, opts)
	defer stop()

	clientCfg, err := opts.ClientTLSConfig()
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer conn.Close()
	c := &txConn{t: t, conn: conn, br: newBufReader(conn)}
	for _, tc := range []struct{ cmd, want string }{
		{"PING", "+PONG"},
		{"SET k v", "+OK"},
		{"GET k", "v"},
	} {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("over TLS %q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A plain-TCP client is not served: its command text is read as a TLS record and
	// rejected, so it never gets a reply.
	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer plain.Close()
	plain.Write([]byte("PING\r\n"))
	plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := plain.Read(make([]byte, 16)); err == nil && n > 0 {
		t.Errorf("a plain-TCP client got %d bytes of reply from a TLS listener", n)
	}
}

// TestTLSRejectsUntrustedCertificate is the negative case for the replica dialer: it
// verifies the peer, so a certificate signed by a root it does not know must fail.
// Without verification, anything that answered on the master's address could feed a
// replica a write stream.
func TestTLSRejectsUntrustedCertificate(t *testing.T) {
	serverCert, serverKey := writeSelfSignedCert(t)
	_, addr, stop := startTLSServer(t, TLSOptions{CertFile: serverCert, KeyFile: serverKey})
	defer stop()

	otherCert, _ := writeSelfSignedCert(t) // a different, unrelated CA
	cfg, err := TLSOptions{CAFile: otherCert}.ClientTLSConfig()
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	conn, err := tls.Dial("tcp", addr, cfg)
	if err == nil {
		conn.Close()
		t.Fatal("dialing with an untrusted root succeeded; the peer was not verified")
	}
}

// TestReplicationOverTLS covers the replication half: a replica dials its master over
// TLS and converges, and the two directions are independent -- this replica serves its
// own clients over plain TCP.
func TestReplicationOverTLS(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	opts := TLSOptions{CertFile: certPath, KeyFile: keyPath, CAFile: certPath}
	_, masterAddr, stopM := startTLSServer(t, opts)
	defer stopM()

	masterCfg, err := opts.ClientTLSConfig()
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	mconn, err := tls.Dial("tcp", masterAddr, masterCfg)
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	defer mconn.Close()
	mc := &txConn{t: t, conn: mconn, br: newBufReader(mconn)}
	mc.cmd("SET before v1")

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	if err := replica.EnableMasterTLS(opts); err != nil {
		t.Fatalf("EnableMasterTLS: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr) // plain TCP to the replica: the two ends are separate
	defer rc.close()
	waitFor(t, "the snapshot to arrive over TLS", func() bool {
		return rc.cmd("GET before") == "v1"
	})
	mc.cmd("SET after v2")
	waitFor(t, "a live write to arrive over TLS", func() bool {
		return rc.cmd("GET after") == "v2"
	})
}

// TestIncompleteTLSOptionsAreRejected pins the refusal to half-configure TLS. Falling
// back to plain TCP would leave an operator who supplied only a certificate serving
// unencrypted traffic on the port they meant to protect, with nothing to tell them.
func TestIncompleteTLSOptionsAreRejected(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	for _, tc := range []struct {
		name string
		opts TLSOptions
	}{
		{"cert without key", TLSOptions{CertFile: certPath}},
		{"key without cert", TLSOptions{KeyFile: keyPath}},
	} {
		if _, err := tc.opts.ServerTLSConfig(); err == nil {
			t.Errorf("%s: ServerTLSConfig succeeded; want ErrIncompleteTLS", tc.name)
		}
		if !tc.opts.Enabled() {
			t.Errorf("%s: Enabled() = false; half-configured TLS still means TLS was asked for", tc.name)
		}
	}
	// Nothing configured at all is not an error: it is the default, plain TCP.
	if (TLSOptions{}).Enabled() {
		t.Error("an empty TLSOptions reports TLS enabled")
	}
}
