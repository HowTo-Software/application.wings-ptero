package uploadsession

import (
	"bufio"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// copyBuffers holds the buffers used to stream chunks to disk. Memory used by an
// upload is therefore bounded by (chunks in flight) x 256 KiB, regardless of the
// chunk or file size.
var copyBuffers = sync.Pool{New: func() any { b := make([]byte, 256<<10); return &b }}

// persisted is the JSON stored for a session, outside of the server's volume.
type persisted struct {
	Version     int       `json:"version"`
	ServerUUID  string    `json:"server_uuid"`
	UserUUID    string    `json:"user_uuid"`
	SessionID   string    `json:"session_id"`
	Directory   string    `json:"directory"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ChunkSize   int64     `json:"chunk_size"`
	Replace     bool      `json:"replace"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	HashedBytes int64     `json:"hashed_bytes"`
	// HashState is the marshaled state of the incremental SHA-256 after HashedBytes.
	HashState []byte  `json:"hash_state,omitempty"`
	Completed *Result `json:"completed,omitempty"`
}

// Session is one resumable upload.
type Session struct {
	m  *Manager
	fs FS
	p  Params

	chunkSize int64
	dir       string // where this session's state files live

	mu        sync.Mutex
	created   time.Time
	updated   time.Time
	chunks    map[int]string // received chunk index -> sha256 hex of its content
	inflight  map[int]struct{}
	hasher    hash.Hash
	hashed    int64
	file      File
	completed *Result
	closed    bool

	// slots limits chunks being received at the same time.
	slots chan struct{}
}

func newSession(m *Manager, fs FS, p Params, chunkSize int64, dir string) *Session {
	now := time.Now().UTC()
	return &Session{
		m:         m,
		fs:        fs,
		p:         p,
		chunkSize: chunkSize,
		dir:       dir,
		created:   now,
		updated:   now,
		chunks:    make(map[int]string),
		inflight:  make(map[int]struct{}),
		hasher:    sha256.New(),
		slots:     make(chan struct{}, m.limits.MaxParallelChunks),
	}
}

func (s *Session) metaPath() string   { return filepath.Join(s.dir, s.p.SessionID+".json") }
func (s *Session) chunksPath() string { return filepath.Join(s.dir, s.p.SessionID+".chunks") }

func (s *Session) chunkCount() int {
	if s.p.Size == 0 {
		return 0
	}
	return int((s.p.Size + s.chunkSize - 1) / s.chunkSize)
}

// chunkBounds returns the offset and exact length chunk i must have.
func (s *Session) chunkBounds(i int) (off, n int64) {
	off = int64(i) * s.chunkSize
	n = s.chunkSize
	if rest := s.p.Size - off; rest < n {
		n = rest
	}
	return off, n
}

// Params returns the immutable parameters of the session.
func (s *Session) Params() Params {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.p
}

// Info returns a snapshot of the session.
func (s *Session) Info() Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoLocked()
}

func (s *Session) receivedIndexesLocked() []int {
	idx := make([]int, 0, len(s.chunks))
	for i := range s.chunks {
		idx = append(idx, i)
	}
	sort.Ints(idx)
	return idx
}

func (s *Session) infoLocked() Info {
	idx := s.receivedIndexesLocked()
	var bytes int64
	for _, i := range idx {
		_, n := s.chunkBounds(i)
		bytes += n
	}
	return Info{
		SessionID:     s.p.SessionID,
		Name:          s.p.Name,
		Directory:     s.p.dir(),
		Size:          s.p.Size,
		ChunkSize:     s.chunkSize,
		ChunkCount:    s.chunkCount(),
		MaxParallel:   cap(s.slots),
		Received:      compress(idx),
		ReceivedBytes: bytes,
		HashedBytes:   s.hashed,
		Completed:     s.completed,
		ExpiresAt:     s.updated.Add(s.m.limits.SessionTTL),
	}
}

// ensureFileLocked opens the shared part file. All chunk writers of a session use
// this one handle so the disk quota sees the file grow only once.
func (s *Session) ensureFileLocked() (File, error) {
	if s.file != nil {
		return s.file, nil
	}
	f, err := s.fs.OpenPart(s.p.PartPath())
	if err != nil {
		return nil, err
	}
	s.file = f
	return f, nil
}

func (s *Session) closeFileLocked() {
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

// PutChunk stores one chunk. The body is streamed straight to the part file at
// the chunk's offset while its SHA-256 is computed; the chunk only counts as
// received after its data was flushed to disk and, when wantSHA is given, matched
// that digest.
//
// Sending a chunk that was already received with the same digest is a no-op.
func (s *Session) PutChunk(index int, body io.Reader, contentLength int64, wantSHA string) (ChunkResult, error) {
	if index < 0 || index >= s.chunkCount() {
		return ChunkResult{}, fmt.Errorf("%w: index %d out of range", ErrBadChunk, index)
	}
	off, want := s.chunkBounds(index)
	if contentLength != want {
		return ChunkResult{}, fmt.Errorf("%w: chunk %d must be exactly %d bytes (Content-Length was %d)", ErrBadChunk, index, want, contentLength)
	}
	wantSHA = strings.ToLower(strings.TrimSpace(wantSHA))
	if wantSHA != "" && !validDigest(wantSHA) {
		return ChunkResult{}, fmt.Errorf("%w: malformed sha256", ErrBadChunk)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ChunkResult{}, ErrNotFound
	}
	if s.completed != nil {
		s.mu.Unlock()
		return ChunkResult{}, fmt.Errorf("%w: upload already completed", ErrConflict)
	}
	if prev, ok := s.chunks[index]; ok {
		s.mu.Unlock()
		if wantSHA == "" || prev == wantSHA {
			return ChunkResult{Index: index, Bytes: want, SHA256: prev, Already: true}, nil
		}
		return ChunkResult{}, fmt.Errorf("%w: chunk %d was already received with different content", ErrConflict, index)
	}
	if _, ok := s.inflight[index]; ok {
		s.mu.Unlock()
		return ChunkResult{}, ErrBusy
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.mu.Unlock()
		return ChunkResult{}, ErrBusy
	}
	s.inflight[index] = struct{}{}
	file, err := s.ensureFileLocked()
	s.touchLocked()
	s.mu.Unlock()

	release := func() {
		s.mu.Lock()
		delete(s.inflight, index)
		s.mu.Unlock()
		<-s.slots
	}

	if err != nil {
		release()
		return ChunkResult{}, s.mapDiskError(err)
	}

	digest, err := s.receive(file, off, want, body)
	if err == nil && wantSHA != "" && digest != wantSHA {
		err = fmt.Errorf("%w: chunk %d is %s on the server, client sent %s", ErrChecksum, index, digest, wantSHA)
	}
	if err == nil {
		// Never acknowledge data that is only in the page cache: a crash would turn
		// the acknowledgement into silent corruption.
		err = file.Sync()
	}
	if err != nil {
		release()
		return ChunkResult{}, s.mapDiskError(err)
	}

	s.mu.Lock()
	delete(s.inflight, index)
	defer func() { s.mu.Unlock(); <-s.slots }()

	if s.closed {
		return ChunkResult{}, ErrNotFound
	}
	s.chunks[index] = digest
	if err := s.appendChunkLogLocked(index, digest); err != nil {
		delete(s.chunks, index)
		return ChunkResult{}, err
	}
	if err := s.advanceLocked(); err != nil {
		delete(s.chunks, index)
		return ChunkResult{}, err
	}
	if err := s.persistLocked(); err != nil {
		return ChunkResult{}, err
	}
	return ChunkResult{Index: index, Bytes: want, SHA256: digest}, nil
}

// receive streams exactly n bytes from body into file at off and returns their SHA-256.
func (s *Session) receive(file File, off, n int64, body io.Reader) (string, error) {
	h := sha256.New()
	bufp := copyBuffers.Get().(*[]byte)
	defer copyBuffers.Put(bufp)

	// Read one byte more than expected so an oversized body is detected, not truncated.
	written, err := io.CopyBuffer(&offsetWriter{f: file, off: off}, io.TeeReader(io.LimitReader(body, n+1), h), *bufp)
	switch {
	case err != nil:
		return "", err
	case written < n:
		return "", fmt.Errorf("%w: got %d of %d bytes", ErrShortBody, written, n)
	case written > n:
		return "", fmt.Errorf("%w: body is longer than %d bytes", ErrBadChunk, n)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type offsetWriter struct {
	f   File
	off int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

// advanceLocked hashes, in order, every received chunk that directly follows the
// part of the file already hashed. Out-of-order chunks simply wait until the gap
// before them is filled, so with parallel uploads almost all hashing happens while
// the upload is still running instead of in a final pass.
func (s *Session) advanceLocked() error {
	if s.hashed >= s.p.Size {
		return nil
	}
	file, err := s.ensureFileLocked()
	if err != nil {
		return err
	}
	for s.hashed < s.p.Size {
		idx := int(s.hashed / s.chunkSize)
		if _, ok := s.chunks[idx]; !ok {
			return nil
		}
		off, n := s.chunkBounds(idx)
		// Hash into a copy so a failed read cannot leave the state half-advanced.
		h := sha256.New()
		if st, ok := s.hasher.(encoding.BinaryMarshaler); ok {
			state, merr := st.MarshalBinary()
			if merr != nil {
				return merr
			}
			if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
				return err
			}
		}
		bufp := copyBuffers.Get().(*[]byte)
		copied, err := io.CopyBuffer(h, io.NewSectionReader(file, off, n), *bufp)
		copyBuffers.Put(bufp)
		if err != nil {
			return err
		}
		if copied != n {
			return fmt.Errorf("%w: chunk %d is %d bytes on disk, expected %d", ErrCorrupt, idx, copied, n)
		}
		s.hasher = h
		s.hashed = off + n
	}
	return nil
}

// Complete verifies that everything arrived, checks the whole-file SHA-256 (against
// wantSHA when given) and atomically moves the part file to its final name.
//
// It is idempotent: repeating it after a success returns the same result, so a
// client that lost the response can simply ask again.
func (s *Session) Complete(wantSHA string) (Result, error) {
	wantSHA = strings.ToLower(strings.TrimSpace(wantSHA))
	if wantSHA != "" && !validDigest(wantSHA) {
		return Result{}, fmt.Errorf("%w: malformed sha256", ErrBadChunk)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.completed != nil {
		return *s.completed, nil
	}
	if s.closed {
		return Result{}, ErrNotFound
	}
	if len(s.inflight) > 0 {
		return Result{}, ErrBusy
	}
	if missing := s.missingLocked(); len(missing) > 0 {
		return Result{}, &IncompleteError{Missing: missing}
	}
	if err := s.fs.IsIgnored(s.p.FinalPath()); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrIgnored, err)
	}

	file, err := s.ensureFileLocked()
	if err != nil {
		return Result{}, s.mapDiskError(err)
	}
	size, err := file.Size()
	if err != nil {
		return Result{}, err
	}
	if size != s.p.Size {
		return Result{}, fmt.Errorf("%w: part file is %d bytes, expected %d", ErrCorrupt, size, s.p.Size)
	}
	if err := s.advanceLocked(); err != nil {
		return Result{}, err
	}
	if s.hashed != s.p.Size {
		return Result{}, fmt.Errorf("%w: hashed %d of %d bytes", ErrCorrupt, s.hashed, s.p.Size)
	}

	sum := hex.EncodeToString(s.hasher.Sum(nil))
	if wantSHA != "" && wantSHA != sum {
		return Result{}, fmt.Errorf("%w: file is %s on the server, client expected %s", ErrChecksum, sum, wantSHA)
	}

	if err := file.Sync(); err != nil {
		return Result{}, err
	}
	s.closeFileLocked()

	if err := s.fs.Replace(s.p.PartPath(), s.p.FinalPath(), s.p.Replace); err != nil {
		return Result{}, err
	}

	res := Result{
		Name:        s.p.Name,
		Directory:   s.p.dir(),
		Size:        s.p.Size,
		SHA256:      sum,
		CompletedAt: time.Now().UTC(),
	}
	s.completed = &res
	s.touchLocked()
	_ = os.Remove(s.chunksPath())
	// The file is already in place; failing to record the tombstone only means a
	// repeated "complete" would report not-found instead of the result.
	_ = s.persistLocked()
	return res, nil
}

// missingLocked returns inclusive ranges of chunk indexes not received yet.
func (s *Session) missingLocked() [][2]int {
	var missing []int
	for i := 0; i < s.chunkCount(); i++ {
		if _, ok := s.chunks[i]; !ok {
			missing = append(missing, i)
		}
	}
	return compress(missing)
}

// SetReplace updates whether finishing may replace an existing file. The permission
// comes from the token of the request being served, which is the authority at that
// moment: a user who lacked it when the upload started may have been granted it since.
func (s *Session) SetReplace(replace bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.p.Replace = replace
}

// Abort discards the session: its part file (freeing the quota) and its state.
func (s *Session) Abort() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	s.closeFileLocked()

	var err error
	if s.completed == nil {
		err = s.fs.Remove(s.p.PartPath())
	}
	_ = os.Remove(s.metaPath())
	_ = os.Remove(s.chunksPath())
	s.m.forget(s)
	return err
}

// releaseIdle closes the part file's handle when nothing has used it for a while.
func (s *Session) releaseIdle(idle time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil && len(s.inflight) == 0 && time.Since(s.updated) > idle {
		s.closeFileLocked()
	}
}

func (s *Session) touchLocked() { s.updated = time.Now().UTC() }

func (s *Session) mapDiskError(err error) error {
	if err == nil {
		return nil
	}
	if s.fs.IsDiskSpace(err) {
		return fmt.Errorf("%w: %v", ErrDiskSpace, err)
	}
	return err
}

func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

/* ------------------------------ persistence ------------------------------- */

func (s *Session) persistLocked() error {
	state, err := s.hasher.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return err
	}
	data, err := json.Marshal(persisted{
		Version:     Version,
		ServerUUID:  s.p.ServerUUID,
		UserUUID:    s.p.UserUUID,
		SessionID:   s.p.SessionID,
		Directory:   s.p.dir(),
		Name:        s.p.Name,
		Size:        s.p.Size,
		ChunkSize:   s.chunkSize,
		Replace:     s.p.Replace,
		CreatedAt:   s.created,
		UpdatedAt:   s.updated,
		HashedBytes: s.hashed,
		HashState:   state,
		Completed:   s.completed,
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	tmp := s.metaPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.metaPath())
}

// appendChunkLogLocked records a received chunk. The log is append-only, so the
// cost of recording a chunk does not grow with the number of chunks.
func (s *Session) appendChunkLogLocked(index int, digest string) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(s.chunksPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := fmt.Fprintf(f, "%d %s\n", index, digest)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	return werr
}

// loadSession restores a session from its state files. It returns ErrNotFound when
// there is none and ErrCorrupt when the files cannot be trusted.
func loadSession(m *Manager, fs FS, serverUUID, id, dir string) (*Session, error) {
	raw, err := os.ReadFile(filepath.Join(dir, id+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var d persisted
	if err := json.Unmarshal(raw, &d); err != nil || d.SessionID != id || d.ServerUUID != serverUUID || d.ChunkSize <= 0 {
		return nil, ErrCorrupt
	}

	p := Params{
		ServerUUID: d.ServerUUID, UserUUID: d.UserUUID, SessionID: d.SessionID,
		Directory: d.Directory, Name: d.Name, Size: d.Size, Replace: d.Replace,
	}
	if err := p.Validate(); err != nil {
		return nil, ErrCorrupt
	}

	s := newSession(m, fs, p, d.ChunkSize, dir)
	s.created, s.updated, s.completed = d.CreatedAt, d.UpdatedAt, d.Completed
	if s.completed != nil {
		return s, nil
	}

	if err := s.loadChunkLog(); err != nil {
		return nil, err
	}
	if len(d.HashState) > 0 {
		if err := s.hasher.(encoding.BinaryUnmarshaler).UnmarshalBinary(d.HashState); err != nil {
			return nil, ErrCorrupt
		}
		s.hashed = d.HashedBytes
	}

	// The part file lives in the server's volume, where it can be deleted or changed.
	// Trust only what is still verifiably there; anything else is sent again.
	size, exists, err := fs.StatPart(p.PartPath())
	if err != nil {
		return nil, err
	}
	var highest int64
	for i := range s.chunks {
		if off, n := s.chunkBounds(i); off+n > highest {
			highest = off + n
		}
	}
	if !exists || size < highest {
		s.chunks = make(map[int]string)
		s.hasher = sha256.New()
		s.hashed = 0
		_ = os.Remove(s.chunksPath())
	}
	return s, nil
}

func (s *Session) loadChunkLog() error {
	f, err := os.Open(s.chunksPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 {
			continue // a torn last line after a crash: that chunk is simply sent again
		}
		i, err := strconv.Atoi(fields[0])
		if err != nil || i < 0 || i >= s.chunkCount() || !validDigest(fields[1]) {
			continue
		}
		s.chunks[i] = fields[1]
	}
	return sc.Err()
}

// readPersisted reads a session's JSON without building a Session.
func readPersisted(p string) (persisted, error) {
	var d persisted
	raw, err := os.ReadFile(p)
	if err != nil {
		return d, err
	}
	err = json.Unmarshal(raw, &d)
	return d, err
}
