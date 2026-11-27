package uploadsession

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testChunk  = 1024
	testServer = "11111111-1111-4111-8111-111111111111"
	testUser   = "22222222-2222-4222-8222-222222222222"
)

var errFakeDisk = errors.New("fake: out of disk")

// fakeFS is a directory on disk standing in for a server's rooted filesystem.
type fakeFS struct {
	root    string
	limit   int64 // 0 = unlimited; checked against the highest offset written
	ignored string
}

func newFakeFS(t *testing.T) *fakeFS { return &fakeFS{root: t.TempDir()} }

func (f *fakeFS) abs(p string) string { return filepath.Join(f.root, filepath.FromSlash(p)) }

func (f *fakeFS) HasSpaceFor(size int64) error {
	if f.limit > 0 && size > f.limit {
		return errFakeDisk
	}
	return nil
}

func (f *fakeFS) IsIgnored(paths ...string) error {
	for _, p := range paths {
		if f.ignored != "" && strings.Contains(p, f.ignored) {
			return errors.New("denylisted")
		}
	}
	return nil
}

type fakeFile struct {
	*os.File
	fs *fakeFS
}

func (f *fakeFile) WriteAt(p []byte, off int64) (int, error) {
	if f.fs.limit > 0 && off+int64(len(p)) > f.fs.limit {
		return 0, errFakeDisk
	}
	return f.File.WriteAt(p, off)
}

func (f *fakeFile) Size() (int64, error) {
	st, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (f *fakeFS) OpenPart(p string) (File, error) {
	if err := os.MkdirAll(filepath.Dir(f.abs(p)), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(f.abs(p), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &fakeFile{File: file, fs: f}, nil
}

func (f *fakeFS) StatPart(p string) (int64, bool, error) {
	st, err := os.Stat(f.abs(p))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return st.Size(), true, nil
}

func (f *fakeFS) Remove(p string) error {
	if err := os.Remove(f.abs(p)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (f *fakeFS) Replace(part, final string, replace bool) error {
	if st, err := os.Stat(f.abs(final)); err == nil {
		if !replace || !st.Mode().IsRegular() {
			return ErrExists
		}
	}
	return os.Rename(f.abs(part), f.abs(final))
}

func (f *fakeFS) IsDiskSpace(err error) bool { return errors.Is(err, errFakeDisk) }

func (f *fakeFS) read(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(f.abs(p))
	require.NoError(t, err)
	return b
}

func (f *fakeFS) exists(p string) bool {
	_, err := os.Stat(f.abs(p))
	return err == nil
}

/* ---------------------------------- helpers ---------------------------------- */

func newManager(t *testing.T) (*Manager, string) {
	t.Helper()
	root := t.TempDir()
	m := NewManager(root, Limits{ChunkSize: testChunk, MaxSessionsPerServer: 4, MaxParallelChunks: 8})
	// Windows cannot delete a directory that still holds open files.
	t.Cleanup(m.Close)
	return m, root
}

func sessionID(n byte) string { return strings.Repeat(fmt.Sprintf("%02x", n), 16) }

func params(id string, size int64) Params {
	return Params{
		ServerUUID: testServer, UserUUID: testUser, SessionID: id,
		Directory: "/mods/backups", Name: "backup.rar", Size: size, Replace: true,
	}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func chunkOf(data []byte, i int) []byte {
	start := i * testChunk
	end := min(start+testChunk, len(data))
	return data[start:end]
}

// putChunk sends one chunk, backing off and retrying when the server says it is busy
// (the HTTP layer turns that into 429), exactly like a real client does.
func putChunk(t *testing.T, s *Session, data []byte, i int) error {
	t.Helper()
	c := chunkOf(data, i)
	for attempt := 0; ; attempt++ {
		_, err := s.PutChunk(i, bytes.NewReader(c), int64(len(c)), sum(c))
		if errors.Is(err, ErrBusy) && attempt < 2000 {
			time.Sleep(time.Millisecond)
			continue
		}
		return err
	}
}

func chunkCount(size int) int { return (size + testChunk - 1) / testChunk }

/* ----------------------------------- tests ----------------------------------- */

func TestParallelOutOfOrderUploadHashesTheWholeFile(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*37+123) // 38 chunks, the last one short

	s, created, err := m.CreateOrResume(fs, params(sessionID(1), int64(len(data))))
	require.NoError(t, err)
	require.True(t, created)

	order := make([]int, chunkCount(len(data)))
	for i := range order {
		order[i] = i
	}
	// Deterministic shuffle so the test is reproducible.
	for i := len(order) - 1; i > 0; i-- {
		j, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		order[i], order[j.Int64()] = order[j.Int64()], order[i]
	}

	var wg sync.WaitGroup
	jobs := make(chan int)
	errs := make(chan error, len(order))
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				errs <- putChunk(t, s, data, i)
			}
		}()
	}
	for _, i := range order {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	res, err := s.Complete("")
	require.NoError(t, err)
	require.Equal(t, sum(data), res.SHA256)
	require.EqualValues(t, len(data), res.Size)
	require.Equal(t, data, fs.read(t, "/mods/backups/backup.rar"))
	require.False(t, fs.exists(s.Params().PartPath()), "part file must be gone")
}

func TestResumeAfterWingsRestart(t *testing.T) {
	m, root := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*10+7)
	id := sessionID(2)

	s, _, err := m.CreateOrResume(fs, params(id, int64(len(data))))
	require.NoError(t, err)
	for _, i := range []int{0, 1, 2, 5} { // 0..2 are contiguous and get hashed; 5 waits
		require.NoError(t, putChunk(t, s, data, i))
	}
	require.EqualValues(t, 3*testChunk, s.Info().HashedBytes)
	s.mu.Lock()
	s.closeFileLocked()
	s.mu.Unlock()

	// "Restart": a brand new manager over the same state directory.
	m2 := NewManager(root, Limits{ChunkSize: testChunk})
	t.Cleanup(m2.Close)
	s2, created, err := m2.CreateOrResume(fs, params(id, int64(len(data))))
	require.NoError(t, err)
	require.False(t, created, "must resume, not start over")
	require.Equal(t, [][2]int{{0, 2}, {5, 5}}, s2.Info().Received)
	require.EqualValues(t, 3*testChunk, s2.Info().HashedBytes, "hash state must be restored, not recomputed")

	for i := 0; i < chunkCount(len(data)); i++ {
		if _, ok := s2.chunks[i]; !ok {
			require.NoError(t, putChunk(t, s2, data, i))
		}
	}
	res, err := s2.Complete("")
	require.NoError(t, err)
	require.Equal(t, sum(data), res.SHA256, "incremental hash across a restart must equal the one-shot hash")
	require.Equal(t, data, fs.read(t, "/mods/backups/backup.rar"))
}

func TestChunkIntegrityAndIdempotency(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*3)
	s, _, err := m.CreateOrResume(fs, params(sessionID(3), int64(len(data))))
	require.NoError(t, err)

	// Wrong digest: rejected and NOT recorded as received.
	c := chunkOf(data, 0)
	_, err = s.PutChunk(0, bytes.NewReader(c), int64(len(c)), sum([]byte("something else")))
	require.ErrorIs(t, err, ErrChecksum)
	require.Empty(t, s.Info().Received)

	// The same chunk can then be sent correctly, and sending it again is a no-op.
	require.NoError(t, putChunk(t, s, data, 0))
	res, err := s.PutChunk(0, bytes.NewReader(c), int64(len(c)), sum(c))
	require.NoError(t, err)
	require.True(t, res.Already)

	// Same index, different content: refused rather than silently overwritten.
	other := randomBytes(t, testChunk)
	_, err = s.PutChunk(0, bytes.NewReader(other), testChunk, sum(other))
	require.ErrorIs(t, err, ErrConflict)
}

func TestBadBodies(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*2)
	s, _, err := m.CreateOrResume(fs, params(sessionID(4), int64(len(data))))
	require.NoError(t, err)

	// Client disconnects early.
	_, err = s.PutChunk(0, io.LimitReader(bytes.NewReader(data), testChunk-1), testChunk, "")
	require.ErrorIs(t, err, ErrShortBody)

	// Body longer than declared.
	_, err = s.PutChunk(0, bytes.NewReader(randomBytes(t, testChunk+5)), testChunk, "")
	require.ErrorIs(t, err, ErrBadChunk)

	// Declared length does not match the chunk geometry.
	_, err = s.PutChunk(0, bytes.NewReader(data[:100]), 100, "")
	require.ErrorIs(t, err, ErrBadChunk)

	// Index out of range; malformed digest.
	_, err = s.PutChunk(9, bytes.NewReader(nil), 0, "")
	require.ErrorIs(t, err, ErrBadChunk)
	_, err = s.PutChunk(0, bytes.NewReader(data[:testChunk]), testChunk, "xyz")
	require.ErrorIs(t, err, ErrBadChunk)

	require.Empty(t, s.Info().Received, "no failed chunk may count as received")
	// ...and the slot/inflight bookkeeping must not leak: a proper upload still works.
	require.NoError(t, putChunk(t, s, data, 0))
	require.NoError(t, putChunk(t, s, data, 1))
	_, err = s.Complete("")
	require.NoError(t, err)
}

func TestCompleteReportsMissingChunks(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*6)
	s, _, err := m.CreateOrResume(fs, params(sessionID(5), int64(len(data))))
	require.NoError(t, err)
	for _, i := range []int{0, 1, 4} {
		require.NoError(t, putChunk(t, s, data, i))
	}

	_, err = s.Complete("")
	var inc *IncompleteError
	require.ErrorAs(t, err, &inc)
	require.Equal(t, [][2]int{{2, 3}, {5, 5}}, inc.Missing)
	require.False(t, fs.exists("/mods/backups/backup.rar"), "an incomplete upload must never appear under its final name")
}

func TestWholeFileChecksumFromClient(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*2+1)
	s, _, _ := m.CreateOrResume(fs, params(sessionID(6), int64(len(data))))
	for i := 0; i < chunkCount(len(data)); i++ {
		require.NoError(t, putChunk(t, s, data, i))
	}

	_, err := s.Complete(sum([]byte("not the file")))
	require.ErrorIs(t, err, ErrChecksum)
	require.False(t, fs.exists("/mods/backups/backup.rar"))

	res, err := s.Complete(sum(data))
	require.NoError(t, err)
	require.Equal(t, sum(data), res.SHA256)
}

func TestCompleteIsIdempotent(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk+10)
	s, _, _ := m.CreateOrResume(fs, params(sessionID(7), int64(len(data))))
	require.NoError(t, putChunk(t, s, data, 0))
	require.NoError(t, putChunk(t, s, data, 1))

	first, err := s.Complete("")
	require.NoError(t, err)
	second, err := s.Complete("")
	require.NoError(t, err)
	require.Equal(t, first, second)

	// Also across a restart, from the tombstone.
	m2 := NewManager(m.root, Limits{ChunkSize: testChunk})
	t.Cleanup(m2.Close)
	// "complete" is addressed to an existing session (the HTTP layer uses Get), not to a new upload.
	s2, err := m2.Get(fs, testServer, testUser, sessionID(7))
	require.NoError(t, err)
	third, err := s2.Complete("")
	require.NoError(t, err)
	require.Equal(t, first.SHA256, third.SHA256)
}

func TestUploadingTheSameFileAgainAfterCompletionStartsAFreshUpload(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk+5)
	id := sessionID(95)

	first, _, err := m.CreateOrResume(fs, params(id, int64(len(data))))
	require.NoError(t, err)
	require.NoError(t, putChunk(t, first, data, 0))
	require.NoError(t, putChunk(t, first, data, 1))
	_, err = first.Complete("")
	require.NoError(t, err)

	// The very same file is picked again within the tombstone's lifetime (same id): that is a NEW upload.
	second, created, err := m.CreateOrResume(fs, params(id, int64(len(data))))
	require.NoError(t, err)
	require.True(t, created, "a completed session must not be resumed")
	require.Empty(t, second.Info().Received)
	require.Nil(t, second.Info().Completed)

	require.NoError(t, putChunk(t, second, data, 0))
	require.NoError(t, putChunk(t, second, data, 1))
	res, err := second.Complete("")
	require.NoError(t, err)
	require.Equal(t, sum(data), res.SHA256)
	require.Equal(t, data, fs.read(t, "/mods/backups/backup.rar"))
}

func TestReplacementRules(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	require.NoError(t, os.MkdirAll(fs.abs("/mods/backups"), 0o755))
	require.NoError(t, os.WriteFile(fs.abs("/mods/backups/backup.rar"), []byte("old"), 0o644))
	data := randomBytes(t, testChunk+1)

	// Without permission to replace the old file survives and the upload is kept.
	p := params(sessionID(8), int64(len(data)))
	p.Replace = false
	s, _, _ := m.CreateOrResume(fs, p)
	require.NoError(t, putChunk(t, s, data, 0))
	require.NoError(t, putChunk(t, s, data, 1))
	_, err := s.Complete("")
	require.ErrorIs(t, err, ErrExists)
	require.Equal(t, []byte("old"), fs.read(t, "/mods/backups/backup.rar"))
	require.True(t, fs.exists(s.Params().PartPath()), "the uploaded data must not be lost")

	// A new token with permission to replace completes the very same session.
	p.Replace = true
	s, created, err := m.CreateOrResume(fs, p)
	require.NoError(t, err)
	require.False(t, created)
	_, err = s.Complete("")
	require.NoError(t, err)
	require.Equal(t, data, fs.read(t, "/mods/backups/backup.rar"))
}

func TestIsolation(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	id := sessionID(9)
	_, _, err := m.CreateOrResume(fs, params(id, 100))
	require.NoError(t, err)

	// Another user cannot see, resume or touch it - and learns nothing about it.
	_, err = m.Get(fs, testServer, "99999999-9999-4999-8999-999999999999", id)
	require.ErrorIs(t, err, ErrNotFound)
	other := params(id, 100)
	other.UserUUID = "99999999-9999-4999-8999-999999999999"
	_, _, err = m.CreateOrResume(fs, other)
	require.ErrorIs(t, err, ErrNotFound)

	// The same id on another server is a different, nonexistent session.
	_, err = m.Get(fs, "33333333-3333-4333-8333-333333333333", testUser, id)
	require.ErrorIs(t, err, ErrNotFound)

	// Same id but a different file is a conflict, not a hijack.
	different := params(id, 101)
	_, _, err = m.CreateOrResume(fs, different)
	require.ErrorIs(t, err, ErrConflict)
}

func TestParamValidationBlocksPathTricks(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b", strings.Repeat("x", 256)} {
		p := params(sessionID(10), 10)
		p.Name = name
		_, _, err := m.CreateOrResume(fs, p)
		require.ErrorIs(t, err, ErrInvalidParams, "name %q", name)
	}
	for _, id := range []string{"", "short", strings.Repeat("g", 32), "../../" + strings.Repeat("a", 26)} {
		p := params(id, 10)
		_, _, err := m.CreateOrResume(fs, p)
		require.ErrorIs(t, err, ErrInvalidParams, "id %q", id)
	}

	// A hostile directory is cleaned to stay under the server root.
	p := params(sessionID(10), 3)
	p.Directory = "/../../etc"
	require.Equal(t, "/etc/backup.rar", p.FinalPath())
	require.Equal(t, "/etc/.upload-"+sessionID(10)+".part", p.PartPath())
}

func TestDenylistAndLimits(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	fs.ignored = "secret"
	p := params(sessionID(11), 10)
	p.Name = "secret.txt"
	_, _, err := m.CreateOrResume(fs, p)
	require.ErrorIs(t, err, ErrIgnored)

	m.limits.MaxSize = 100
	_, _, err = m.CreateOrResume(newFakeFS(t), params(sessionID(12), 101))
	require.ErrorIs(t, err, ErrTooLarge)

	m.limits.MaxSize = 0
	for i := 0; i < m.limits.MaxSessionsPerServer; i++ {
		_, _, err = m.CreateOrResume(newFakeFS(t), params(sessionID(byte(20+i)), 10))
		require.NoError(t, err)
	}
	_, _, err = m.CreateOrResume(newFakeFS(t), params(sessionID(40), 10))
	require.ErrorIs(t, err, ErrTooManySess)
}

func TestQuota(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	fs.limit = testChunk * 4

	// Does not fit at all: refused before a single byte is accepted.
	_, _, err := m.CreateOrResume(fs, params(sessionID(13), testChunk*5))
	require.ErrorIs(t, err, ErrDiskSpace)

	// Fits when declared, but the disk fills up while uploading.
	fs.limit = testChunk * 4
	s, _, err := m.CreateOrResume(fs, params(sessionID(14), testChunk*4))
	require.NoError(t, err)
	fs.limit = testChunk*2 + 10 // someone else used the space meanwhile
	data := randomBytes(t, testChunk*4)
	require.NoError(t, putChunk(t, s, data, 0))
	require.NoError(t, putChunk(t, s, data, 1))
	err = putChunk(t, s, data, 2)
	require.ErrorIs(t, err, ErrDiskSpace)
	require.Equal(t, [][2]int{{0, 1}}, s.Info().Received)

	// Aborting gives everything back.
	require.NoError(t, s.Abort())
	require.False(t, fs.exists(s.Params().PartPath()))
}

func TestPartFileDeletedBehindOurBack(t *testing.T) {
	m, root := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*4)
	id := sessionID(15)
	s, _, _ := m.CreateOrResume(fs, params(id, int64(len(data))))
	require.NoError(t, putChunk(t, s, data, 0))
	require.NoError(t, putChunk(t, s, data, 1))
	s.mu.Lock()
	s.closeFileLocked()
	s.mu.Unlock()

	// The server's owner removes the part file from the file manager.
	require.NoError(t, os.Remove(fs.abs(s.Params().PartPath())))

	m2 := NewManager(root, Limits{ChunkSize: testChunk})
	t.Cleanup(m2.Close)
	s2, _, err := m2.CreateOrResume(fs, params(id, int64(len(data))))
	require.NoError(t, err)
	require.Empty(t, s2.Info().Received, "chunks whose data is gone must be sent again, not trusted")
	require.Zero(t, s2.Info().HashedBytes)

	for i := 0; i < 4; i++ {
		require.NoError(t, putChunk(t, s2, data, i))
	}
	res, err := s2.Complete("")
	require.NoError(t, err)
	require.Equal(t, sum(data), res.SHA256)
}

func TestEdgeSizes(t *testing.T) {
	for _, size := range []int{0, 1, testChunk - 1, testChunk, testChunk + 1, testChunk * 3} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m, _ := newManager(t)
			fs := newFakeFS(t)
			data := randomBytes(t, size)
			s, _, err := m.CreateOrResume(fs, params(sessionID(50), int64(size)))
			require.NoError(t, err)
			for i := 0; i < chunkCount(size); i++ {
				require.NoError(t, putChunk(t, s, data, i))
			}
			res, err := s.Complete("")
			require.NoError(t, err)
			require.Equal(t, sum(data), res.SHA256)
			require.Equal(t, data, fs.read(t, "/mods/backups/backup.rar"))
		})
	}
}

func TestSweepCleansEverythingItShould(t *testing.T) {
	root := t.TempDir()
	m := NewManager(root, Limits{ChunkSize: testChunk, SessionTTL: time.Hour, CompletedTTL: time.Minute})
	t.Cleanup(m.Close)
	fs := newFakeFS(t)
	resolve := func(uuid string) (FS, bool) { return fs, uuid == testServer }

	data := randomBytes(t, testChunk*2)
	mk := func(n byte) *Session {
		s, _, err := m.CreateOrResume(fs, params(sessionID(n), int64(len(data))))
		require.NoError(t, err)
		return s
	}
	fresh, stale, done := mk(60), mk(61), mk(62)
	for _, s := range []*Session{fresh, stale, done} {
		require.NoError(t, putChunk(t, s, data, 0))
	}
	require.NoError(t, putChunk(t, done, data, 1))
	_, err := done.Complete("")
	require.NoError(t, err)

	// Age two of them.
	age := func(s *Session, by time.Duration) {
		s.mu.Lock()
		s.updated = time.Now().Add(-by)
		if s.completed != nil {
			s.completed.CompletedAt = time.Now().Add(-by)
		}
		require.NoError(t, s.persistLocked())
		s.mu.Unlock()
	}
	age(stale, 2*time.Hour)
	age(done, 2*time.Minute)

	// A session of a server that was deleted.
	orphanDir := filepath.Join(root, "44444444-4444-4444-8444-444444444444")
	require.NoError(t, os.MkdirAll(orphanDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(orphanDir, sessionID(70)+".json"), []byte(`{}`), 0o600))

	removed := m.Sweep(resolve)
	require.GreaterOrEqual(t, removed, 3)

	require.True(t, fs.exists(fresh.Params().PartPath()), "a session in use must survive")
	require.False(t, fs.exists(stale.Params().PartPath()), "an idle session's data must be removed to free the quota")
	require.False(t, fileExists(stale.metaPath()))
	require.False(t, fileExists(done.metaPath()), "an expired tombstone must go")
	require.True(t, fs.exists("/mods/backups/backup.rar"), "the finished file itself is never touched")
	require.False(t, fileExists(orphanDir), "state of a deleted server must go")
}

func TestConcurrentChunksOfDifferentSessionsDoNotInterfere(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)

	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			data := randomBytes(t, testChunk*8+n)
			p := params(sessionID(byte(80+n)), int64(len(data)))
			p.Name = fmt.Sprintf("file%d.bin", n)
			s, _, err := m.CreateOrResume(fs, p)
			require.NoError(t, err)
			var inner sync.WaitGroup
			for i := 0; i < chunkCount(len(data)); i++ {
				inner.Add(1)
				go func(i int) { defer inner.Done(); require.NoError(t, putChunk(t, s, data, i)) }(i)
			}
			inner.Wait()
			res, err := s.Complete("")
			require.NoError(t, err)
			require.Equal(t, sum(data), res.SHA256)
			require.Equal(t, data, fs.read(t, "/mods/backups/"+p.Name))
		}(n)
	}
	wg.Wait()
}

func TestSameChunkTwiceAtOnceIsRefused(t *testing.T) {
	m, _ := newManager(t)
	fs := newFakeFS(t)
	data := randomBytes(t, testChunk*2)
	s, _, _ := m.CreateOrResume(fs, params(sessionID(90), int64(len(data))))

	// First request: blocks inside the body until released.
	pr, pw := io.Pipe()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := s.PutChunk(0, pr, testChunk, "")
		done <- err
	}()
	<-started
	require.Eventually(t, func() bool { return s.hasInflight() }, time.Second, time.Millisecond)

	_, err := s.PutChunk(0, bytes.NewReader(data[:testChunk]), testChunk, "")
	require.ErrorIs(t, err, ErrBusy)
	// Completing while a chunk is in flight is refused too.
	_, err = s.Complete("")
	require.ErrorIs(t, err, ErrBusy)

	_, _ = pw.Write(data[:testChunk])
	_ = pw.Close()
	require.NoError(t, <-done)
}
