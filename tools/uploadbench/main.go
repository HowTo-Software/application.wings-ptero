//go:build unix

// Command uploadbench measures upload throughput of a real Wings HTTP stack: the
// legacy POST /upload/file against the chunked session API with 1..N parallel
// connections, over HTTP/1.1 (one TCP connection per request, like a browser with
// several parallel requests) or HTTP/2 (all requests multiplexed on ONE connection,
// like a browser talking to an h2 server).
//
// It runs as two processes so CPU/RAM/disk of the server can be measured on their own:
//
//	uploadbench serve --dir /tmp/bench --listen 127.0.0.1:18443 --token secret
//	uploadbench run   --url https://127.0.0.1:18443 --token secret --mode chunked --proto h1 --conns 4 --size-mib 192
//
// The server is the real router.Configure from this repository on a real (temporary)
// server volume; only the Panel and Docker are absent. Results are measurements of
// WHATEVER network path lies between the two processes; on loopback with `tc netem`
// that is an emulation, not the real internet.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/internal/database"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/router"
	"github.com/pterodactyl/wings/router/tokens"
	"github.com/pterodactyl/wings/server"
)

const (
	serverUUID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	userUUID   = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: uploadbench serve|run [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		serve(os.Args[2:])
	case "run":
		run(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown command", os.Args[1])
		os.Exit(2)
	}
}

/* --------------------------------- server --------------------------------- */

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dir := fs.String("dir", "", "working directory (state and server volume); required")
	listen := fs.String("listen", "127.0.0.1:18443", "address to listen on")
	token := fs.String("token", "", "shared secret used to sign/verify JWTs; required")
	limitMiB := fs.Int64("upload-limit-mib", 100000, "api.upload_limit in MiB")
	chunk := fs.Int64("chunk-size", 16<<20, "upload session chunk size in bytes")
	parallel := fs.Int("max-parallel", 8, "max parallel chunks per session")
	h2Window := fs.Int("h2-window-mib", 0, "api.upload_sessions.http2_receive_window_mib (0 = Go default)")
	_ = fs.Parse(args)
	if *dir == "" || *token == "" {
		fs.Usage()
		os.Exit(2)
	}

	cfg, err := config.NewAtPath(filepath.Join(*dir, "config.yml"))
	must(err)
	cfg.AuthenticationToken = *token
	cfg.PanelLocation = "http://panel.test"
	cfg.System.RootDirectory = *dir
	cfg.System.Data = filepath.Join(*dir, "volumes")
	cfg.System.User.Uid = os.Getuid()
	cfg.System.User.Gid = os.Getgid()
	cfg.Api.UploadLimit = *limitMiB
	cfg.Api.UploadSessions.Enabled = true
	cfg.Api.UploadSessions.ChunkSize = *chunk
	cfg.Api.UploadSessions.MaxParallelChunks = *parallel
	cfg.Api.UploadSessions.HTTP2ReceiveWindowMiB = *h2Window
	config.Set(cfg)
	must(os.MkdirAll(cfg.System.Data, 0o755))
	must(database.Initialize())

	settings, err := json.Marshal(map[string]any{"uuid": serverUUID, "build": map[string]any{"disk_space": 0}})
	must(err)
	manager := server.NewEmptyManager(nil)
	s, err := manager.InitServer(serverUUID, remote.ServerConfigurationResponse{Settings: settings, ProcessConfiguration: &remote.ProcessConfiguration{}})
	must(err)
	manager.Add(s)

	cert, err := selfSigned()
	must(err)
	srv := router.NewServer(*listen, router.Configure(manager, nil), &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	ln, err := net.Listen("tcp", *listen)
	must(err)

	fmt.Println("READY pid", os.Getpid(), "volume", s.Filesystem().Path())
	must(srv.ServeTLS(ln, "", ""))
}

func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "uploadbench"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

/* --------------------------------- client --------------------------------- */

type result struct {
	Mode       string  `json:"mode"`
	Proto      string  `json:"proto"`
	Conns      int     `json:"conns"`
	SizeMiB    int64   `json:"size_mib"`
	Seconds    float64 `json:"seconds"`
	SentBytes  int64   `json:"sent_bytes"`
	MBPerSec   float64 `json:"mb_per_s"`
	Completed  bool    `json:"completed"`
	SHAMatches *bool   `json:"sha_matches,omitempty"`
	Error      string  `json:"error,omitempty"`
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	url := fs.String("url", "https://127.0.0.1:18443", "Wings base URL")
	secret := fs.String("token", "", "shared secret (same as serve)")
	mode := fs.String("mode", "chunked", "legacy | chunked")
	proto := fs.String("proto", "h1", "h1 | h2")
	conns := fs.Int("conns", 1, "parallel connections/requests (chunked mode)")
	sizeMiB := fs.Int64("size-mib", 192, "file size in MiB")
	chunk := fs.Int64("chunk-size", 16<<20, "chunk size in bytes (must match the server)")
	maxSeconds := fs.Int("max-seconds", 120, "stop the transfer after this many seconds and report what was sent")
	_ = fs.Parse(args)
	if *secret == "" {
		fs.Usage()
		os.Exit(2)
	}

	c := &client{
		base:   strings.TrimRight(*url, "/"),
		algo:   jwt.NewHS256([]byte(*secret)),
		chunk:  *chunk,
		size:   *sizeMiB << 20,
		http:   newHTTPClient(*proto, *conns),
		sent:   new(atomic.Int64),
		sessID: strings.ReplaceAll(uuid.NewString(), "-", ""),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*maxSeconds)*time.Second)
	defer cancel()

	start := time.Now()
	var err error
	var shaOK *bool
	switch *mode {
	case "legacy":
		err = c.legacy(ctx)
	case "chunked":
		shaOK, err = c.chunked(ctx, *conns)
	default:
		fmt.Fprintln(os.Stderr, "unknown mode", *mode)
		os.Exit(2)
	}
	elapsed := time.Since(start).Seconds()

	r := result{
		Mode: *mode, Proto: *proto, Conns: *conns, SizeMiB: *sizeMiB, Seconds: elapsed,
		SentBytes: c.sent.Load(), Completed: err == nil, SHAMatches: shaOK,
	}
	if *mode == "legacy" {
		r.Conns = 1
	}
	r.MBPerSec = float64(r.SentBytes) / elapsed / 1e6
	if err != nil {
		r.Error = err.Error()
	}
	out, _ := json.Marshal(r)
	fmt.Println(string(out))
}

func newHTTPClient(proto string, conns int) *http.Client {
	t := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // benchmark against a self-signed local server
		MaxIdleConnsPerHost: 64,
	}
	if proto == "h2" {
		// ALPN negotiates HTTP/2: every request shares one TCP connection.
		t.ForceAttemptHTTP2 = true
	} else {
		// HTTP/1.1 only: each in-flight request gets its own TCP connection.
		t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		t.MaxConnsPerHost = conns
	}
	return &http.Client{Transport: t}
}

type client struct {
	base   string
	algo   *jwt.HMACSHA
	chunk  int64
	size   int64
	http   *http.Client
	sent   *atomic.Int64
	sessID string
}

func (c *client) sessionToken() string {
	now := time.Now().Add(2 * time.Second) // Wings rejects tokens issued before it booted (second precision)
	p := tokens.UploadSessionPayload{
		Payload: jwt.Payload{IssuedAt: jwt.NumericDate(now), ExpirationTime: jwt.NumericDate(now.Add(time.Hour))},
		Scoped:  tokens.Scoped{Scope: string(tokens.UploadSession)}, ServerUuid: serverUUID, UserUuid: userUUID,
		SessionId: c.sessID, Directory: "/bench", Name: "file.bin", Size: c.size, Replace: true,
	}
	t, err := jwt.Sign(&p, c.algo)
	must(err)
	return string(t)
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (cr countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n.Add(int64(n))
	return n, err
}

// fill writes deterministic pseudo-random bytes for chunk index i (xorshift64*), so the
// client needs no big file on disk and every run sends incompressible data.
func fill(buf []byte, i int) {
	x := uint64(i)*0x9E3779B97F4A7C15 + 0x1234567
	for o := 0; o+8 <= len(buf); o += 8 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		v := x * 0x2545F4914F6CDD1D
		for b := 0; b < 8; b++ {
			buf[o+b] = byte(v >> (8 * b))
		}
	}
}

func (c *client) chunked(ctx context.Context, conns int) (ok *bool, err error) {
	token := func() string { return c.sessionToken() }

	// A run that is cut by the time box (or fails) must not leave its session behind: the node limits
	// unfinished sessions per server, and stale ones would make the following runs fail with HTTP 429.
	defer func() {
		if err == nil {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rq, _ := http.NewRequestWithContext(dctx, http.MethodDelete, c.base+"/upload/session/"+c.sessID+"?token="+token(), nil)
		if rs, derr := c.http.Do(rq); derr == nil {
			_, _ = io.Copy(io.Discard, rs.Body)
			_ = rs.Body.Close()
		}
	}()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/upload/session?token="+token(), nil)
	res, derr := c.http.Do(req)
	if derr != nil {
		return nil, derr
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusCreated && res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create session: HTTP %d", res.StatusCode)
	}

	count := int((c.size + c.chunk - 1) / c.chunk)
	jobs := make(chan int)
	var wg sync.WaitGroup
	var firstErr atomic.Value
	whole := sha256.New()

	for w := 0; w < conns; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				n := min(c.chunk, c.size-int64(i)*c.chunk)
				buf := make([]byte, n)
				fill(buf, i)
				sum := sha256.Sum256(buf)

				url := fmt.Sprintf("%s/upload/session/%s/chunks/%d?token=%s&sha256=%s", c.base, c.sessID, i, token(), hex.EncodeToString(sum[:]))
				rq, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, countingReader{bytes.NewReader(buf), c.sent})
				rq.ContentLength = n
				rs, err := c.http.Do(rq)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				body, _ := io.ReadAll(rs.Body)
				_ = rs.Body.Close()
				if rs.StatusCode != http.StatusOK {
					firstErr.CompareAndSwap(nil, fmt.Errorf("chunk %d: HTTP %d %s", i, rs.StatusCode, strings.TrimSpace(string(body))))
					return
				}
			}
		}()
	}
feed:
	for i := 0; i < count; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return nil, v.(error)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("time box reached: %w", ctx.Err())
	}

	// Whole-file SHA-256 computed independently of the server, then compared with what the server returns.
	for i := 0; i < count; i++ {
		n := min(c.chunk, c.size-int64(i)*c.chunk)
		buf := make([]byte, n)
		fill(buf, i)
		whole.Write(buf)
	}
	want := hex.EncodeToString(whole.Sum(nil))

	cr, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/upload/session/"+c.sessID+"/complete?token="+token(), nil)
	cres, err := c.http.Do(cr)
	if err != nil {
		return nil, err
	}
	defer cres.Body.Close()
	var out struct {
		SHA256 string `json:"sha256"`
		Error  string `json:"error"`
	}
	_ = json.NewDecoder(cres.Body).Decode(&out)
	if cres.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("complete: HTTP %d %s", cres.StatusCode, out.Error)
	}
	matches := out.SHA256 == want
	return &matches, nil
}

func (c *client) legacy(ctx context.Context) error {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		part, err := mw.CreateFormFile("files", "file.bin")
		if err == nil {
			buf := make([]byte, 1<<20)
			for off, i := int64(0), 0; off < c.size && err == nil; i++ {
				n := min(int64(len(buf)), c.size-off)
				fill(buf[:n], i)
				_, err = part.Write(buf[:n])
				off += n
			}
		}
		if err == nil {
			err = mw.Close()
		}
		_ = pw.CloseWithError(err)
	}()

	now := time.Now().Add(2 * time.Second)
	p := tokens.UploadPayload{
		Payload: jwt.Payload{IssuedAt: jwt.NumericDate(now), ExpirationTime: jwt.NumericDate(now.Add(time.Hour))},
		Scoped:  tokens.Scoped{Scope: string(tokens.FileUpload)}, ServerUuid: serverUUID, UserUuid: userUUID, UniqueId: uuid.NewString(),
	}
	tok, err := jwt.Sign(&p, c.algo)
	must(err)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/upload/file?directory=/bench&token="+string(tok), countingReader{pr, c.sent})
	req.Header.Set("Content-Type", mw.FormDataContentType())
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("legacy upload: HTTP %d %s", res.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
