# Homebrew formula for shardkv.
#
# This file is the source of truth for the tap repository Black-third/homebrew-tap: copy
# it to Formula/shardkv.rb there and `brew install black-third/tap/shardkv` works (brew
# strips the `homebrew-` prefix from the repository name).
#
#     brew tap black-third/tap
#     brew install shardkv
#     brew install --HEAD shardkv     # build main from source instead
#
# THE sha256 VALUES BELOW ARE PLACEHOLDERS. They are all zeros, so a `brew install` of
# the stable version fails the checksum until they are filled in -- deliberately, because
# a wrong-but-plausible digest is worse than an obviously fake one. Two ways to fill
# them:
#
#   * let GoReleaser do it. `brews:` in /.goreleaser.yml renders this same formula from
#     the release's archives and the digests in checksums.txt, and pushes it to the tap.
#     That is the intended path; see the SKELETON comment there for what has to exist
#     first.
#   * by hand, from the release page: `shasum -a 256 shardkv_<version>_<os>_<arch>.tar.gz`,
#     or copy the line out of checksums.txt.
#
# Keep `version` and the four archive names in step with `archives.name_template` in
# /.goreleaser.yml -- {{ .ProjectName }}_{{ .Version }}_{{ .Os }}_{{ .Arch }}. If that
# template changes, every url here breaks.
class Shardkv < Formula
  desc "Concurrent, sharded, Redis-wire-compatible in-memory data-structure server"
  homepage "https://github.com/Black-third/shardkv"
  version "0.3.0"
  license "MIT"

  # Stable installs a pre-built binary: there is nothing to compile, no toolchain to
  # download, and the archive is the exact artifact the release checksums cover.
  on_macos do
    on_arm do
      url "https://github.com/Black-third/shardkv/releases/download/v#{version}/shardkv_#{version}_darwin_arm64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # PLACEHOLDER
    end
    on_intel do
      url "https://github.com/Black-third/shardkv/releases/download/v#{version}/shardkv_#{version}_darwin_amd64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # PLACEHOLDER
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/Black-third/shardkv/releases/download/v#{version}/shardkv_#{version}_linux_arm64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # PLACEHOLDER
    end
    on_intel do
      url "https://github.com/Black-third/shardkv/releases/download/v#{version}/shardkv_#{version}_linux_amd64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000" # PLACEHOLDER
    end
  end

  # --HEAD builds the branch from source, which needs a Go toolchain -- and only then, so
  # a stable install stays a download. The module has no third-party dependencies, so
  # this build reaches the network for the toolchain and nothing else.
  head do
    url "https://github.com/Black-third/shardkv.git", branch: "main"

    depends_on "go" => :build
  end

  def install
    if build.head?
      # std_go_args already passes -trimpath and -o bin/shardkv. CGO is off so the
      # result is a static binary, as the release archives are.
      ENV["CGO_ENABLED"] = "0"
      system "go", "build", *std_go_args(ldflags: "-s -w"), "./cmd/shardkv"
    else
      bin.install "shardkv"
    end

    # The AOF has to live somewhere the server may write, which is not the Cellar. This
    # is the directory the service block below points at.
    (var/"shardkv").mkpath
  end

  # `brew services start shardkv` -- persistence on, and the port is 6380 rather than
  # 6379 so it does not collide with a Redis on the same machine.
  service do
    run [opt_bin/"shardkv", "-addr", ":6380", "-aof", var/"shardkv/shardkv.aof"]
    keep_alive true
    working_dir var/"shardkv"
    log_path var/"log/shardkv.log"
    error_log_path var/"log/shardkv.log"
  end

  def caveats
    <<~EOS
      shardkv listens on :6380 by default, so it does not collide with a Redis on 6379.
      Persistence is off unless you pass -aof:

        shardkv -addr :6380 -aof #{var}/shardkv/shardkv.aof

      Or run it as a service, which does that for you:

        brew services start shardkv
        redis-cli -p 6380 ping
    EOS
  end

  # Starting the server and getting +PONG back over a socket is the only test that
  # proves the installed binary runs on this machine and speaks the protocol. Raw RESP
  # rather than a redis-cli, so the test depends on nothing that is not already here.
  test do
    port = free_port
    pid = fork do
      exec bin/"shardkv", "-addr", ":#{port}", "-aof", testpath/"shardkv.aof"
    end

    begin
      require "socket"
      sock = nil
      # The listener is bound before the accept loop starts, but the process still has
      # to get that far; retry briefly rather than sleeping a fixed guess.
      10.times do
        sock = TCPSocket.open("127.0.0.1", port)
        break
      rescue Errno::ECONNREFUSED
        sleep 0.5
      end
      refute_nil sock, "shardkv did not start listening on port #{port}"

      sock.write "PING\r\n"
      assert_equal "+PONG", sock.gets.chomp

      sock.write "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
      assert_equal "+OK", sock.gets.chomp

      sock.write "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
      assert_equal "$3", sock.gets.chomp
      assert_equal "bar", sock.gets.chomp
      sock.close
    ensure
      Process.kill "TERM", pid
      Process.wait pid
    end
  end
end
