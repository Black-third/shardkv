package server

import (
	"crypto/subtle"

	"github.com/Black-third/shardkv/internal/resp"
)

// errNoAuth is the reply every command gets on a connection that has not
// authenticated against a password-protected server. The wording is Redis's, so a
// client library recognizes it and re-authenticates instead of surfacing it as an
// unknown failure.
const errNoAuth = "NOAUTH Authentication required."

// errWrongPass is Redis's reply to a bad username/password pair.
const errWrongPass = "WRONGPASS invalid username-password pair or user is disabled."

// defaultUser is the only user this server knows. There is no ACL system here, so
// AUTH accepts a username purely for compatibility with clients that always send the
// two-argument form; any other username is rejected rather than silently ignored,
// which would authenticate a caller as somebody it did not ask to be.
const defaultUser = "default"

// noAuthCommands are the commands an unauthenticated connection may still run.
// AUTH and HELLO are how it authenticates at all; PING lets a client and a load
// balancer probe liveness without credentials; QUIT lets it hang up cleanly; and
// RESET only discards connection state, so refusing it would be pointless. Anything
// else -- including PSYNC, checked separately in handle -- gets NOAUTH.
var noAuthCommands = map[string]bool{
	"AUTH":  true,
	"HELLO": true,
	"PING":  true,
	"QUIT":  true,
	"RESET": true,
}

func init() {
	registerSession("AUTH", -2, cmdAuth)
}

// SetRequirePass sets the password AUTH must present; an empty string leaves the
// server open. It may be called at runtime (CONFIG SET requirepass), which is why
// the value is stored atomically. Connections already authenticated stay
// authenticated, exactly as in Redis: the password guards new connections, and
// dropping every established one on a config change would be a denial of service
// triggered by an administrative command.
func (s *Server) SetRequirePass(pass string) { s.requirepass.Store(&pass) }

// RequirePass reports the configured password, or "" when the server is open.
func (s *Server) RequirePass() string {
	if p := s.requirepass.Load(); p != nil {
		return *p
	}
	return ""
}

// SetMasterAuth sets the password this server presents to its master when it
// replicates. A replica of a password-protected master needs its own credential:
// the master rejects PSYNC from an unauthenticated connection just as it rejects any
// other command.
func (s *Server) SetMasterAuth(pass string) { s.masterauth.Store(&pass) }

// MasterAuth reports the password used when connecting to a master.
func (s *Server) MasterAuth() string {
	if p := s.masterauth.Load(); p != nil {
		return *p
	}
	return ""
}

// needsAuth reports whether the connection must authenticate before running
// anything. A session that was created while no password was set stays
// authenticated (see newSession), so the common case -- an open server -- is a
// single atomic load and a boolean test.
func (s *Server) needsAuth(sess *session) bool {
	if sess.authenticated {
		return false
	}
	return s.RequirePass() != ""
}

// cmdAuth implements AUTH password and AUTH username password.
//
// The comparison is constant-time: a byte-by-byte compare that returns early leaks
// how much of a guess was correct, which turns an offline-strength secret into an
// online, per-byte search. The password is never logged or echoed back in an error,
// for the same reason a server log is not a place to keep credentials.
func cmdAuth(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if len(args) > 3 {
		w.WriteError("ERR wrong number of arguments for 'auth' command")
		return
	}
	user, pass := defaultUser, args[1]
	twoArg := len(args) == 3
	if twoArg {
		user, pass = string(args[1]), args[2]
	}

	want := s.RequirePass()
	if want == "" {
		w.WriteError("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")
		return
	}
	if user != defaultUser {
		w.WriteError(errWrongPass)
		return
	}
	if subtle.ConstantTimeCompare(pass, []byte(want)) != 1 {
		// The ACL form reports the pair as wrong (WRONGPASS, what Redis 6+ replies);
		// the legacy single-argument form keeps the older "invalid password" wording
		// that clients predating ACLs match on.
		if twoArg {
			w.WriteError(errWrongPass)
		} else {
			w.WriteError("ERR invalid password")
		}
		return
	}
	sess.authenticated = true
	w.WriteSimple("OK")
}
