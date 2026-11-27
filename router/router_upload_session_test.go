package router

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/router/tokens"
)

const sessionChunk = 4096

// newSessionTest starts the router with chunked uploads enabled and a small chunk size.
func newSessionTest(t *testing.T, uploadLimit, diskLimit int64) *uploadTest {
	t.Helper()
	u := newUploadTest(t, uploadLimit, diskLimit)

	cfg := config.Get()
	// Session state is kept under the Wings root directory, which defaults to /var/lib/pterodactyl.
	cfg.System.RootDirectory = t.TempDir()
	cfg.Api.UploadSessions.Enabled = true
	cfg.Api.UploadSessions.ChunkSize = sessionChunk
	cfg.Api.UploadSessions.MaxParallelChunks = 8
	cfg.Api.UploadSessions.MaxSessionsPerServer = 4
	cfg.Api.UploadSessions.SessionTTLHours = 24
	config.Set(cfg)

	// A fresh manager for every test (it is process-wide and reads the config once).
	uploadSessionsMu.Lock()
	uploadSessionsMgr = nil
	uploadSessionsMu.Unlock()
	t.Cleanup(func() {
		uploadSessionsMu.Lock()
		if uploadSessionsMgr != nil {
			uploadSessionsMgr.Close()
			uploadSessionsMgr = nil
		}
		uploadSessionsMu.Unlock()
	})
	return u
}

type sessionSpec struct {
	id        string
	directory string
	name      string
	size      int64
	replace   bool
}

func newSessionSpec(name string, size int) sessionSpec {
	return sessionSpec{id: strings.ReplaceAll(uuid.NewString(), "-", ""), directory: "/mods/backups", name: name, size: int64(size), replace: true}
}

func (u *uploadTest) sessionToken(t *testing.T, sp sessionSpec, mutate ...func(*tokens.UploadSessionPayload)) string {
	t.Helper()
	// Tokens issued before Wings boots are rejected, and the issued time only has second precision.
	now := time.Now().Add(2 * time.Second)
	payload := tokens.UploadSessionPayload{
		Payload: jwt.Payload{
			IssuedAt:       jwt.NumericDate(now),
			ExpirationTime: jwt.NumericDate(now.Add(15 * time.Minute)),
		},
		Scoped:     tokens.Scoped{Scope: string(tokens.UploadSession)},
		ServerUuid: uploadTestServerUuid,
		UserUuid:   "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		SessionId:  sp.id,
		Directory:  sp.directory,
		Name:       sp.name,
		Size:       sp.size,
		Replace:    sp.replace,
	}
	for _, m := range mutate {
		m(&payload)
	}
	token, err := jwt.Sign(&payload, config.GetJwtAlgorithm())
	require.NoError(t, err)
	return string(token)
}

func (u *uploadTest) call(t *testing.T, method, path string, body []byte) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u.url+path, r)
	require.NoError(t, err)
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

func (u *uploadTest) create(t *testing.T, sp sessionSpec) (int, map[string]any) {
	return u.call(t, http.MethodPost, "/upload/session?token="+u.sessionToken(t, sp), nil)
}

func (u *uploadTest) putChunk(t *testing.T, sp sessionSpec, data []byte, index int, withDigest bool) (int, map[string]any) {
	t.Helper()
	start := index * sessionChunk
	chunk := data[start:min(start+sessionChunk, len(data))]
	q := ""
	if withDigest {
		sum := sha256.Sum256(chunk)
		q = "&sha256=" + hex.EncodeToString(sum[:])
	}
	for attempt := 0; ; attempt++ {
		status, out := u.call(t, http.MethodPut, "/upload/session/"+sp.id+"/chunks/"+itoa(index)+"?token="+u.sessionToken(t, sp)+q, chunk)
		if status == http.StatusTooManyRequests && attempt < 500 {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		return status, out
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func shaHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func (u *uploadTest) final(sp sessionSpec) string {
	return filepath.Join(u.root, filepath.FromSlash(sp.directory), sp.name)
}

/* ------------------------------------------------------------------------------ */

func TestSessionFullParallelUpload(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, sessionChunk*25+77)
	sp := newSessionSpec("backup.rar", len(data))

	status, out := u.create(t, sp)
	require.Equal(t, http.StatusCreated, status, out)
	require.Equal(t, false, out["resumed"])

	chunks := (len(data) + sessionChunk - 1) / sessionChunk
	var wg sync.WaitGroup
	idx := make(chan int)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				st, o := u.putChunk(t, sp, data, i, true)
				require.Equal(t, http.StatusOK, st, o)
			}
		}()
	}
	for i := chunks - 1; i >= 0; i-- { // deliberately backwards
		idx <- i
	}
	close(idx)
	wg.Wait()

	// The final name does not exist until the upload is completed and verified.
	_, err := os.Stat(u.final(sp))
	require.ErrorIs(t, err, os.ErrNotExist)

	status, out = u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+u.sessionToken(t, sp), []byte(`{"sha256":"`+shaHex(data)+`"}`))
	require.Equal(t, http.StatusOK, status, out)
	require.Equal(t, shaHex(data), out["sha256"])

	got, err := os.ReadFile(u.final(sp))
	require.NoError(t, err)
	require.Equal(t, data, got)

	// No hidden part file is left behind.
	entries, err := os.ReadDir(filepath.Dir(u.final(sp)))
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Completing again is safe (the response may have been lost).
	status, out = u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusOK, status, out)
	require.Equal(t, shaHex(data), out["sha256"])
}

func TestSessionResumeAfterRestart(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, sessionChunk*6+5)
	sp := newSessionSpec("big.bin", len(data))

	status, _ := u.create(t, sp)
	require.Equal(t, http.StatusCreated, status)
	for _, i := range []int{0, 1, 4} {
		st, o := u.putChunk(t, sp, data, i, true)
		require.Equal(t, http.StatusOK, st, o)
	}

	// Wings restarts (and the browser was closed in between): nothing in memory survives.
	uploadSessionsMu.Lock()
	uploadSessionsMgr.Close()
	uploadSessionsMgr = nil
	uploadSessionsMu.Unlock()

	// The "same file again" yields the same session id, and the node reports what it already has.
	status, out := u.create(t, sp)
	require.Equal(t, http.StatusOK, status, out)
	require.Equal(t, true, out["resumed"])
	sess := out["session"].(map[string]any)
	require.Equal(t, []any{[]any{float64(0), float64(1)}, []any{float64(4), float64(4)}}, sess["received"])

	for _, i := range []int{2, 3, 5, 6} {
		st, o := u.putChunk(t, sp, data, i, true)
		require.Equal(t, http.StatusOK, st, o)
	}
	status, out = u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusOK, status, out)
	require.Equal(t, shaHex(data), out["sha256"], "the hash must cover data uploaded before AND after the restart")
}

func TestSessionTokenRules(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, sessionChunk*2)
	sp := newSessionSpec("a.bin", len(data))

	// Expired.
	expired := u.sessionToken(t, sp, func(p *tokens.UploadSessionPayload) {
		p.ExpirationTime = jwt.NumericDate(time.Now().Add(-time.Minute))
	})
	status, out := u.call(t, http.MethodPost, "/upload/session?token="+expired, nil)
	require.Equal(t, http.StatusUnauthorized, status)
	require.Equal(t, "E_TOKEN", out["code"])

	// Garbage / missing.
	status, _ = u.call(t, http.MethodPost, "/upload/session?token=nope", nil)
	require.Equal(t, http.StatusUnauthorized, status)
	status, _ = u.call(t, http.MethodPost, "/upload/session", nil)
	require.Equal(t, http.StatusUnauthorized, status)

	// A legacy single-use upload token cannot drive a session.
	legacy := u.sessionToken(t, sp, func(p *tokens.UploadSessionPayload) { p.Scope = string(tokens.FileUpload) })
	status, out = u.call(t, http.MethodPost, "/upload/session?token="+legacy, nil)
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "E_REVOKED", out["code"])

	// Unknown server.
	other := u.sessionToken(t, sp, func(p *tokens.UploadSessionPayload) { p.ServerUuid = "ffffffff-ffff-4fff-8fff-ffffffffffff" })
	status, _ = u.call(t, http.MethodPost, "/upload/session?token="+other, nil)
	require.Equal(t, http.StatusNotFound, status)

	// A token only works for ITS session id.
	status, _ = u.create(t, sp)
	require.Equal(t, http.StatusCreated, status)
	wrong := strings.ReplaceAll(uuid.NewString(), "-", "")
	status, out = u.call(t, http.MethodGet, "/upload/session/"+wrong+"?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, "E_SESSION", out["code"])

	// Unlike the legacy token, the same token may be used for many requests.
	tok := u.sessionToken(t, sp)
	for i := 0; i < 3; i++ {
		status, _ = u.call(t, http.MethodGet, "/upload/session/"+sp.id+"?token="+tok, nil)
		require.Equal(t, http.StatusOK, status)
	}

	// Another USER holding a valid token for the same id learns nothing about the session.
	intruder := u.sessionToken(t, sp, func(p *tokens.UploadSessionPayload) { p.UserUuid = "dddddddd-dddd-4ddd-8ddd-dddddddddddd" })
	status, _ = u.call(t, http.MethodGet, "/upload/session/"+sp.id+"?token="+intruder, nil)
	require.Equal(t, http.StatusNotFound, status)
}

func TestSessionRejectsBadChunks(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, sessionChunk*3)
	sp := newSessionSpec("c.bin", len(data))
	status, _ := u.create(t, sp)
	require.Equal(t, http.StatusCreated, status)

	// Wrong digest: 422 and not recorded.
	tok := u.sessionToken(t, sp)
	status, out := u.call(t, http.MethodPut, "/upload/session/"+sp.id+"/chunks/0?sha256="+strings.Repeat("0", 64)+"&token="+tok, data[:sessionChunk])
	require.Equal(t, http.StatusUnprocessableEntity, status, out)
	require.Equal(t, "E_CHECKSUM", out["code"])

	// Wrong size, bad index.
	status, _ = u.call(t, http.MethodPut, "/upload/session/"+sp.id+"/chunks/0?token="+tok, data[:100])
	require.Equal(t, http.StatusBadRequest, status)
	status, _ = u.call(t, http.MethodPut, "/upload/session/"+sp.id+"/chunks/99?token="+tok, data[:sessionChunk])
	require.Equal(t, http.StatusBadRequest, status)
	status, _ = u.call(t, http.MethodPut, "/upload/session/"+sp.id+"/chunks/abc?token="+tok, data[:sessionChunk])
	require.Equal(t, http.StatusBadRequest, status)

	// Completing early lists exactly what is missing and creates nothing.
	st, o := u.putChunk(t, sp, data, 0, true)
	require.Equal(t, http.StatusOK, st, o)
	status, out = u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+tok, nil)
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "E_INCOMPLETE", out["code"])
	require.Equal(t, []any{[]any{float64(1), float64(2)}}, out["missing"])
	_, err := os.Stat(u.final(sp))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSessionLimits(t *testing.T) {
	// Node upload limit (MiB) applies before a single byte is accepted.
	u := newSessionTest(t, 1, 0)
	sp := newSessionSpec("huge.bin", 2<<20)
	status, out := u.create(t, sp)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "E_TOO_LARGE", out["code"])

	// Disk quota (1 MiB) is checked up front too.
	u = newSessionTest(t, 10, 1)
	sp = newSessionSpec("fat.bin", 2<<20)
	status, out = u.create(t, sp)
	require.Equal(t, http.StatusInsufficientStorage, status, out)
	require.Equal(t, "E_NODISK", out["code"])
}

func TestSessionReplaceKeepsModeAndRespectsPermission(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, sessionChunk+9)
	sp := newSessionSpec("run.sh", len(data))
	require.NoError(t, os.MkdirAll(filepath.Dir(u.final(sp)), 0o755))
	require.NoError(t, os.WriteFile(u.final(sp), []byte("old"), 0o750))

	// Without permission to replace: the old file survives and the upload is kept for later.
	deny := sp
	deny.replace = false
	status, _ := u.create(t, deny)
	require.Equal(t, http.StatusCreated, status)
	for i := 0; i < 2; i++ {
		st, o := u.putChunk(t, deny, data, i, true)
		require.Equal(t, http.StatusOK, st, o)
	}
	status, out := u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+u.sessionToken(t, deny), nil)
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "E_EXISTS", out["code"])
	got, _ := os.ReadFile(u.final(sp))
	require.Equal(t, []byte("old"), got)

	// A token that allows replacing finishes the same session, atomically, keeping permissions.
	status, out = u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusOK, status, out)
	got, err := os.ReadFile(u.final(sp))
	require.NoError(t, err)
	require.Equal(t, data, got)
	st, err := os.Stat(u.final(sp))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o750), st.Mode().Perm(), "replacing a file must not reset its permissions")
}

func TestSessionStaysInsideTheServerRoot(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, 100)
	sp := newSessionSpec("x.txt", len(data))
	sp.directory = "/../../../../etc"

	status, _ := u.create(t, sp)
	require.Equal(t, http.StatusCreated, status)
	st, o := u.putChunk(t, sp, data, 0, true)
	require.Equal(t, http.StatusOK, st, o)
	status, out := u.call(t, http.MethodPost, "/upload/session/"+sp.id+"/complete?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusOK, status, out)

	// It landed under the server's own /etc, not the host's.
	_, err := os.Stat(filepath.Join(u.root, "etc", "x.txt"))
	require.NoError(t, err)
}

func TestSessionAbortRemovesPartialData(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	data := randBytes(t, sessionChunk*3)
	sp := newSessionSpec("gone.bin", len(data))
	status, _ := u.create(t, sp)
	require.Equal(t, http.StatusCreated, status)
	st, o := u.putChunk(t, sp, data, 0, true)
	require.Equal(t, http.StatusOK, st, o)

	status, _ = u.call(t, http.MethodDelete, "/upload/session/"+sp.id+"?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusNoContent, status)

	entries, _ := os.ReadDir(filepath.Dir(u.final(sp)))
	require.Empty(t, entries, "the partial file must be removed")
	status, _ = u.call(t, http.MethodGet, "/upload/session/"+sp.id+"?token="+u.sessionToken(t, sp), nil)
	require.Equal(t, http.StatusNotFound, status)
}

// The legacy endpoint must keep working exactly as before next to the new routes.
func TestLegacyUploadStillWorksWithSessionsEnabled(t *testing.T) {
	u := newSessionTest(t, 1, 0)

	status, _ := u.upload(t, map[string][]byte{"a.txt": []byte("legacy")})
	require.Equal(t, http.StatusOK, status)
	got, err := os.ReadFile(filepath.Join(u.root, "uploads", "a.txt"))
	require.NoError(t, err)
	require.Equal(t, "legacy", string(got))
}

func TestSessionsCanBeDisabled(t *testing.T) {
	u := newSessionTest(t, 10, 0)
	cfg := config.Get()
	cfg.Api.UploadSessions.Enabled = false
	config.Set(cfg)

	status, out := u.create(t, newSessionSpec("a.bin", 10))
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "E_DISABLED", out["code"])
	require.Nil(t, uploadCapabilities(), "a disabled node must not advertise the feature")
}

func TestCapabilitiesAdvertisement(t *testing.T) {
	newSessionTest(t, 10240, 0)

	caps := uploadCapabilities()["howto_upload_sessions"].(map[string]any)
	require.EqualValues(t, 1, caps["version"])
	require.EqualValues(t, sessionChunk, caps["chunk_size"])
	require.EqualValues(t, 8, caps["max_parallel_chunks"])
	require.EqualValues(t, int64(10240)*1024*1024, caps["max_size"])
	require.Equal(t, "sha256", caps["hash"])
}
