package server

import (
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

// writeUnknownSubcommand answers a container command that was given a subcommand it
// does not have, with the text Redis 7 uses:
//
//	ERR unknown subcommand 'BOGUS'. Try OBJECT HELP.
//
// It exists because this string was written out by hand at eight call sites and had
// already drifted into four different spellings -- "Unknown subcommand or wrong number
// of arguments for 'X'. Try C HELP.", "Unknown C subcommand or wrong number of arguments
// for 'X'", a lowercased variant, and a bare "syntax error" from XINFO. Every one of them
// was measured against redis:7.2 and every one was wrong. The older long form is what
// Redis's addReplySubcommandSyntaxError used to produce; 7.x shortened it, and a client
// that matches on the message sees the difference.
//
// A container's own name is passed in rather than derived from args[0], because a client
// may spell the command in any case ("object encoding") while the help text names it in
// upper case, as Redis does.
func writeUnknownSubcommand(w *resp.Writer, container string, sub []byte) {
	w.WriteError("ERR unknown subcommand '" + string(sub) + "'. Try " +
		strings.ToUpper(container) + " HELP.")
}

// writeSubcommandSyntaxError answers a subcommand whose *options* are wrong, with Redis's
// addReplySubcommandSyntaxError text:
//
//	ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.
//
// Redis really does use two different messages, and which one applies depends on whether
// the subcommand has optional arguments. `XINFO GROUPS k extra` is an arity error
// ("wrong number of arguments for 'xinfo|groups' command") because GROUPS takes exactly a
// key; `XINFO STREAM k nope` is *this* one, because STREAM accepts an optional FULL and so
// the fourth argument is an unrecognised option rather than a surplus one. Both were
// measured against redis:7.2 -- the distinction is not guessable, and picking one for
// everything is what made the earlier hand-written copies wrong.
//
// The subcommand is echoed as the client spelled it, which is what Redis does.
func writeSubcommandSyntaxError(w *resp.Writer, container string, sub []byte) {
	w.WriteError("ERR unknown subcommand or wrong number of arguments for '" +
		string(sub) + "'. Try " + strings.ToUpper(container) + " HELP.")
}

// writeSubcommandHelp answers <CONTAINER> HELP. Redis replies with an array of lines: a
// header, then two lines per subcommand (its syntax, then an indented description), then
// HELP itself. Clients do not parse this -- redis-cli prints it -- so what matters is that
// it is an array of status lines rather than an error, and that it lists what this server
// actually implements rather than what Redis implements.
//
// A container that answers an error to HELP is the specific bug this exists to stop:
// HELP is the one subcommand every container has, so an error means the container's
// dispatch has no default at all, and every unknown subcommand takes the same wrong path.
func writeSubcommandHelp(w *resp.Writer, container string, lines []string) {
	upper := strings.ToUpper(container)
	w.WriteArrayHeader(len(lines) + 3)
	w.WriteSimple(upper + " <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")
	for _, l := range lines {
		w.WriteSimple(l)
	}
	w.WriteSimple("HELP")
	w.WriteSimple("    Print this help.")
}
