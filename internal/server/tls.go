package server

// Optional TLS for the client listener and for the connection a replica opens to its
// master.
//
// Plain TCP stays the default. Encryption is configuration, not a rebuild: the
// listener is wrapped only when a certificate is supplied, and the replication dialer
// only when replication over TLS is asked for. A deployment that had no certificates
// yesterday behaves exactly as it did.

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
)

// ErrIncompleteTLS reports a certificate given without its key, or the reverse. It is
// an error rather than a silent fallback to plain TCP, because an operator who
// configured half of TLS asked for TLS: quietly serving unencrypted traffic on the
// port they meant to protect is the one outcome they cannot detect.
var ErrIncompleteTLS = errors.New("server: both a TLS certificate and key are required")

// TLSOptions is the certificate material for one direction of TLS. The same shape
// serves the listener and the replication dialer, since both need a keypair to
// present and a root to verify the other side against.
type TLSOptions struct {
	CertFile string // PEM certificate this server presents
	KeyFile  string // PEM private key for CertFile
	CAFile   string // PEM roots used to verify the peer (empty = the system roots)
}

// Enabled reports whether any TLS material was configured.
func (o TLSOptions) Enabled() bool { return o.CertFile != "" || o.KeyFile != "" }

// ServerTLSConfig builds the configuration for the listener. Client certificates are
// requested but not required: a CA here exists to let a replica verify the master,
// and demanding mutual TLS from every redis-cli would make the option unusable for
// the case it is normally wanted for.
func (o TLSOptions) ServerTLSConfig() (*tls.Config, error) {
	cert, err := o.keypair()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{*cert},
		MinVersion:   tls.VersionTLS12,
	}
	if o.CAFile != "" {
		pool, err := certPool(o.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

// ClientTLSConfig builds the configuration a replica uses to dial its master. The
// peer is always verified: a replica that accepted any certificate would accept any
// host claiming to be the master, and would then apply that host's write stream to
// its dataset.
func (o TLSOptions) ClientTLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if o.CertFile != "" || o.KeyFile != "" {
		cert, err := o.keypair()
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{*cert}
	}
	if o.CAFile != "" {
		pool, err := certPool(o.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func (o TLSOptions) keypair() (*tls.Certificate, error) {
	if o.CertFile == "" || o.KeyFile == "" {
		return nil, ErrIncompleteTLS
	}
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}

func certPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("server: no certificates found in " + caFile)
	}
	return pool, nil
}

// EnableTLS makes Listen wrap the listener in TLS built from opts. It must be called
// before Listen, since the wrapping happens as the socket is bound. The options are
// retained so CONFIG GET can report which files are actually in use -- an operator
// debugging a certificate problem needs to know what the server loaded, not what a
// configuration file says it should have.
func (s *Server) EnableTLS(opts TLSOptions) error {
	cfg, err := opts.ServerTLSConfig()
	if err != nil {
		return err
	}
	s.tlsConfig = cfg
	s.tlsOpts = opts
	return nil
}

// EnableMasterTLS makes this server dial its master over TLS. It is separate from
// EnableTLS because the two directions are independent in practice: a replica may need
// to reach an encrypted master while itself serving plain TCP on a private interface,
// and a TLS-terminating master may replicate from a peer that does not speak it.
func (s *Server) EnableMasterTLS(opts TLSOptions) error {
	cfg, err := opts.ClientTLSConfig()
	if err != nil {
		return err
	}
	s.masterTLS = cfg
	s.masterTLSOn = true
	return nil
}

// TLSConfigInUse reports the certificate material the listener loaded, and whether
// replication dials its master over TLS.
func (s *Server) TLSConfigInUse() (opts TLSOptions, replication bool) {
	return s.tlsOpts, s.masterTLSOn
}

// dialMaster opens the connection to a master, over TLS when configured. The
// handshake happens here rather than lazily on first byte so a certificate problem
// surfaces as a failed connection the replication loop retries, not as a corrupt
// stream halfway through a resync.
func (s *Server) dialMaster(addr string) (net.Conn, error) {
	if cfg := s.masterTLS; cfg != nil {
		return tls.Dial("tcp", addr, cfg)
	}
	return net.Dial("tcp", addr)
}
