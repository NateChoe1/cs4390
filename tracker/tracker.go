// Package tracker is the tracker server for the CS 4390 peer-to-peer file
// sharing project. Start it with tracker.Run (see cmd/tracker/main.go).
//
// The tracker keeps one "<filename>.track" file per shared file inside the
// torrents directory. It understands four commands (one per TCP connection):
//
//	createtracker filename filesize description md5 ip port
//	updatetracker filename start end ip port
//	REQ LIST
//	GET filename.track
//
// Every message is a single line, normally wrapped in <...> and ended by \n.
// Each connection is handled by its own goroutine (worker) which closes the
// connection right after the reply has been sent.
package tracker

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------- config ---

// Config holds the values read from the sconfig file.
type Config struct {
	Port        int    // line 1: port the tracker listens on
	Dir         string // line 2: shared directory for .track files
	PeerTimeout int    // line 3 (optional): seconds without an update before a peer is dead
}

const defaultPeerTimeout = 1200 // seconds; a bit above the default 15 min peer refresh

// loadConfig reads sconfig. Blank lines and lines starting with # are ignored.
func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		lines = append(lines, l)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(lines) < 2 {
		return nil, fmt.Errorf("%s needs at least 2 lines: port and shared directory", path)
	}
	port, err := strconv.Atoi(lines[0])
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port %q in %s", lines[0], path)
	}
	cfg := &Config{Port: port, Dir: lines[1], PeerTimeout: defaultPeerTimeout}
	if len(lines) >= 3 {
		t, err := strconv.Atoi(lines[2])
		if err != nil || t < 1 {
			return nil, fmt.Errorf("invalid peer timeout %q in %s", lines[2], path)
		}
		cfg.PeerTimeout = t
	}
	return cfg, nil
}

// --------------------------------------------------------- tracker files ---

// Peer is one "<ip>:<port>:<start>:<end>:<timestamp>" line of a tracker file.
type Peer struct {
	IP    string
	Port  int
	Start int64
	End   int64
	Stamp int64 // unix seconds of last update
}

// Tracker is the in-memory form of a .track file.
type Tracker struct {
	Filename string
	Size     int64
	Desc     string
	MD5      string
	Peers    []Peer
}

// marshal renders the tracker in the required file format (ends with \n).
func (t *Tracker) marshal() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "Filename: %s\n", t.Filename)
	fmt.Fprintf(&b, "Filesize: %d\n", t.Size)
	fmt.Fprintf(&b, "Description: %s\n", t.Desc)
	fmt.Fprintf(&b, "MD5: %s\n", t.MD5)
	b.WriteString("# list of peers follows next\n")
	for _, p := range t.Peers {
		fmt.Fprintf(&b, "%s:%d:%d:%d:%d\n", p.IP, p.Port, p.Start, p.End, p.Stamp)
	}
	return []byte(b.String())
}

// ParseTracker parses .track file content. Lines starting with # are comments.
func ParseTracker(data string) (*Tracker, error) {
	t := &Tracker{}
	sawName, sawSize, sawMD5 := false, false, false
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "Filename:"):
			t.Filename = strings.TrimSpace(strings.TrimPrefix(line, "Filename:"))
			sawName = true
		case strings.HasPrefix(line, "Filesize:"):
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "Filesize:")), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("bad Filesize line")
			}
			t.Size, sawSize = n, true
		case strings.HasPrefix(line, "Description:"):
			t.Desc = strings.TrimSpace(strings.TrimPrefix(line, "Description:"))
		case strings.HasPrefix(line, "MD5:"):
			t.MD5 = strings.TrimSpace(strings.TrimPrefix(line, "MD5:"))
			sawMD5 = true
		default:
			f := strings.Split(line, ":")
			if len(f) != 5 {
				return nil, fmt.Errorf("bad peer line %q", line)
			}
			port, e1 := strconv.Atoi(f[1])
			start, e2 := strconv.ParseInt(f[2], 10, 64)
			end, e3 := strconv.ParseInt(f[3], 10, 64)
			stamp, e4 := strconv.ParseInt(f[4], 10, 64)
			if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
				return nil, fmt.Errorf("bad peer line %q", line)
			}
			t.Peers = append(t.Peers, Peer{f[0], port, start, end, stamp})
		}
	}
	if !sawName || !sawSize || !sawMD5 {
		return nil, fmt.Errorf("missing header fields")
	}
	return t, nil
}

// prune removes peers whose last update is older than timeout seconds.
// It returns true if anything was removed.
func (t *Tracker) prune(now int64, timeout int) bool {
	kept := t.Peers[:0]
	for _, p := range t.Peers {
		if now-p.Stamp <= int64(timeout) {
			kept = append(kept, p)
		} else {
			log.Printf("removed dead peer %s:%d from %s", p.IP, p.Port, t.Filename)
		}
	}
	changed := len(kept) != len(t.Peers)
	t.Peers = kept
	return changed
}

// ------------------------------------------------------------- server ---

// Server bundles configuration and per-file locks.
type Server struct {
	cfg     Config
	mu      sync.Mutex             // guards locks map
	fileMux map[string]*sync.Mutex // one lock per tracker file
}

// lock returns the mutex for a given tracker name.
func (s *Server) lock(name string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.fileMux[name]
	if !ok {
		m = &sync.Mutex{}
		s.fileMux[name] = m
	}
	return m
}

// cleanName strips an optional ".track" suffix and rejects unsafe names
// (path separators, "..") so peers cannot read or write outside torrents/.
func cleanName(name string) (string, bool) {
	name = strings.TrimSuffix(strings.TrimSpace(name), ".track")
	if name == "" || name == "." || name == ".." ||
		strings.ContainsAny(name, `/\`) || name != filepath.Base(name) {
		return "", false
	}
	return name, true
}

func (s *Server) path(name string) string {
	return filepath.Join(s.cfg.Dir, name+".track")
}

func (s *Server) load(name string) (*Tracker, error) {
	data, err := os.ReadFile(s.path(name))
	if err != nil {
		return nil, err
	}
	return ParseTracker(string(data))
}

// save writes the tracker atomically (temp file + rename) so readers never
// see a half-written file.
func (s *Server) save(name string, t *Tracker) error {
	tmp := s.path(name) + ".tmp"
	if err := os.WriteFile(tmp, t.marshal(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(name))
}

func send(conn net.Conn, msg string) {
	if _, err := io.WriteString(conn, msg); err != nil {
		log.Printf("write to %s failed: %v", conn.RemoteAddr(), err)
	}
}

// handleConn is the worker: read one request line, answer it, close.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	line, err := bufio.NewReaderSize(io.LimitReader(conn, 8192), 8192).ReadString('\n')
	if err != nil && strings.TrimSpace(line) == "" {
		log.Printf("no request from %s: %v", conn.RemoteAddr(), err)
		return
	}
	// Strip the <...> wrapper (also tolerates manual input without it).
	line = strings.TrimSpace(line)
	line = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "<"), ">"))
	f := strings.Fields(line)
	if len(f) == 0 {
		send(conn, "<error empty request>\n")
		return
	}

	switch cmd := strings.ToLower(f[0]); {
	case cmd == "req" && len(f) >= 2 && strings.EqualFold(f[1], "list"), cmd == "list":
		s.handleList(conn)
	case cmd == "get":
		s.handleGet(conn, f)
	case cmd == "createtracker":
		s.handleCreate(conn, f)
	case cmd == "updatetracker":
		s.handleUpdate(conn, f)
	default:
		log.Printf("unknown command from %s: %q", conn.RemoteAddr(), line)
		send(conn, "<error unknown command>\n")
	}
}

// handleCreate: createtracker filename filesize description md5 ip port
// The description may contain spaces if a peer did not use underscores, so
// we take the last 3 fields as md5/ip/port and join the middle for the text.
func (s *Server) handleCreate(conn net.Conn, f []string) {
	fail := func(why string) {
		log.Printf("createtracker from %s failed: %s", conn.RemoteAddr(), why)
		send(conn, "<createtracker fail>\n")
	}
	if len(f) < 6 {
		fail("too few fields")
		return
	}
	name, ok := cleanName(f[1])
	size, err := strconv.ParseInt(f[2], 10, 64)
	n := len(f)
	md5sum, ip, portStr := strings.ToLower(f[n-3]), f[n-2], f[n-1]
	port, perr := strconv.Atoi(portStr)
	desc := strings.Join(f[3:n-3], " ")
	_, hexErr := hex.DecodeString(md5sum)
	switch {
	case !ok:
		fail("invalid filename")
		return
	case err != nil || size < 0:
		fail("invalid filesize")
		return
	case len(md5sum) != 32 || hexErr != nil:
		fail("invalid md5")
		return
	case net.ParseIP(ip) == nil:
		fail("invalid ip")
		return
	case perr != nil || port < 1 || port > 65535:
		fail("invalid port")
		return
	}

	m := s.lock(name)
	m.Lock()
	defer m.Unlock()

	if _, err := os.Stat(s.path(name)); err == nil {
		log.Printf("createtracker %s from %s: tracker already exists", name, conn.RemoteAddr())
		send(conn, "<createtracker ferr>\n")
		return
	}
	t := &Tracker{Filename: name, Size: size, Desc: desc, MD5: md5sum,
		Peers: []Peer{{ip, port, 0, size, time.Now().Unix()}}}
	if err := s.save(name, t); err != nil {
		fail("cannot write tracker file: " + err.Error())
		return
	}
	log.Printf("createtracker %s (%d bytes) by %s:%d -> succ", name, size, ip, port)
	send(conn, "<createtracker succ>\n")
}

// handleUpdate: updatetracker filename start end ip port
func (s *Server) handleUpdate(conn net.Conn, f []string) {
	label := "?"
	if len(f) >= 2 {
		label = f[1]
	}
	reply := func(code string) { send(conn, fmt.Sprintf("<updatetracker %s %s>\n", label, code)) }

	if len(f) != 6 {
		log.Printf("updatetracker from %s: wrong number of fields", conn.RemoteAddr())
		reply("fail")
		return
	}
	name, ok := cleanName(f[1])
	start, e1 := strconv.ParseInt(f[2], 10, 64)
	end, e2 := strconv.ParseInt(f[3], 10, 64)
	ip := f[4]
	port, e3 := strconv.Atoi(f[5])
	if !ok || e1 != nil || e2 != nil || e3 != nil || start < 0 || end < start ||
		net.ParseIP(ip) == nil || port < 1 || port > 65535 {
		log.Printf("updatetracker from %s: invalid arguments", conn.RemoteAddr())
		reply("fail")
		return
	}

	m := s.lock(name)
	m.Lock()
	defer m.Unlock()

	t, err := s.load(name)
	if os.IsNotExist(err) {
		log.Printf("updatetracker %s from %s:%d: no such tracker", name, ip, port)
		reply("ferr")
		return
	} else if err != nil {
		log.Printf("updatetracker %s: cannot read tracker: %v", name, err)
		reply("fail")
		return
	}
	if end > t.Size {
		log.Printf("updatetracker %s from %s:%d: range beyond file size", name, ip, port)
		reply("fail")
		return
	}

	now := time.Now().Unix()
	t.prune(now, s.cfg.PeerTimeout) // drop dead peers (never the sender, stamped below)
	found := false
	for i := range t.Peers {
		if t.Peers[i].IP == ip && t.Peers[i].Port == port {
			t.Peers[i].Start, t.Peers[i].End, t.Peers[i].Stamp = start, end, now
			found = true
		}
	}
	if !found {
		t.Peers = append(t.Peers, Peer{ip, port, start, end, now})
	}
	if err := s.save(name, t); err != nil {
		log.Printf("updatetracker %s: cannot write tracker: %v", name, err)
		reply("fail")
		return
	}
	log.Printf("updatetracker %s: %s:%d has bytes %d-%d -> succ", name, ip, port, start, end)
	reply("succ")
}

// handleList: reply with every tracker file (name, size, md5).
func (s *Server) handleList(conn net.Conn) {
	files, _ := filepath.Glob(filepath.Join(s.cfg.Dir, "*.track"))
	var trackers []*Tracker
	for _, p := range files {
		name := strings.TrimSuffix(filepath.Base(p), ".track")
		m := s.lock(name)
		m.Lock()
		t, err := s.load(name)
		m.Unlock()
		if err != nil {
			log.Printf("skipping unreadable tracker %s: %v", p, err)
			continue
		}
		trackers = append(trackers, t)
	}
	sort.Slice(trackers, func(i, j int) bool { return trackers[i].Filename < trackers[j].Filename })

	var b strings.Builder
	fmt.Fprintf(&b, "<REP LIST %d>\n", len(trackers))
	for i, t := range trackers {
		fmt.Fprintf(&b, "<%d %s %d %s>\n", i+1, t.Filename, t.Size, t.MD5)
	}
	b.WriteString("<REP LIST END>\n")
	send(conn, b.String())
	log.Printf("LIST from %s: %d file(s)", conn.RemoteAddr(), len(trackers))
}

// handleGet: GET filename.track  (".track" is optional).
// Reply: <REP GET BEGIN>\n <file content> <REP GET END md5-of-content>\n
func (s *Server) handleGet(conn net.Conn, f []string) {
	if len(f) < 2 {
		send(conn, "<REP GET ERR badrequest>\n")
		return
	}
	name, ok := cleanName(f[1])
	if !ok {
		send(conn, "<REP GET ERR badrequest>\n")
		return
	}
	m := s.lock(name)
	m.Lock()
	t, err := s.load(name)
	if err == nil && t.prune(time.Now().Unix(), s.cfg.PeerTimeout) {
		err = s.save(name, t) // persist the removal of dead peers
	}
	m.Unlock()
	if err != nil {
		log.Printf("GET %s from %s: %v", name, conn.RemoteAddr(), err)
		send(conn, "<REP GET ERR notfound>\n")
		return
	}
	content := t.marshal()
	sum := md5.Sum(content)
	send(conn, "<REP GET BEGIN>\n"+string(content)+"<REP GET END "+hex.EncodeToString(sum[:])+">\n")
	log.Printf("GET %s.track from %s: sent %d peer(s)", name, conn.RemoteAddr(), len(t.Peers))
}

// ----------------------------------------------------------------- Run ---

// Run loads the config file, starts listening and serves peers forever.
// keep=false deletes old .track files first (the final demo needs an empty
// tracker directory). It only returns if a fatal error happens.
func Run(cfgPath string, keep bool) error {
	log.SetFlags(log.Ldate | log.Ltime)
	log.SetPrefix("[tracker] ")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("config error: %w", err)
	}
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return fmt.Errorf("cannot create %s: %w", cfg.Dir, err)
	}
	if !keep {
		old, _ := filepath.Glob(filepath.Join(cfg.Dir, "*.track*"))
		for _, p := range old {
			os.Remove(p)
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return fmt.Errorf("cannot listen on port %d: %w", cfg.Port, err)
	}
	log.Printf("listening on port %d, tracker files in %s, peer timeout %ds",
		cfg.Port, cfg.Dir, cfg.PeerTimeout)

	srv := &Server{cfg: *cfg, fileMux: make(map[string]*sync.Mutex)}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go srv.handleConn(conn) // one worker per peer request
	}
}
