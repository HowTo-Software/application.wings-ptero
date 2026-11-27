// Package uploadsession implements chunked, resumable file uploads for Wings.
//
// A session represents the upload of ONE file of a known size, split into
// fixed-size chunks that can be sent in any order and in parallel, and resumed
// after a connection drop, a Wings restart or the user closing their browser.
//
// Design rules (see docs/uploads/WINGS-RESUMABLE-UPLOADS.md in the panel repo):
//   - data is written to a hidden ".upload-<id>.part" file next to the target, never to
//     the target itself, so the target only ever refers to a complete file;
//   - session state (chunk digests, incremental hash state) lives OUTSIDE the server's
//     volume, where the game server's processes cannot touch it;
//   - every disk access goes through the server's own rooted filesystem, so a session
//     can never reach another server's files;
//   - chunks are streamed to disk, never buffered whole: memory per active chunk is a
//     single copy buffer;
//   - the whole file is hashed with SHA-256 incrementally, as the contiguous prefix of
//     chunks becomes available, so completing an upload needs no extra pass over the data.
package uploadsession

import (
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"time"
)

// DefaultChunkSize is the chunk size used unless configured otherwise: 16 MiB.
const DefaultChunkSize int64 = 16 << 20

// Version is the version of the session protocol advertised to the panel.
const Version = 1

// Sentinel errors. The HTTP layer maps them to status codes.
var (
	ErrNotFound      = errors.New("upload session not found")
	ErrConflict      = errors.New("upload session conflicts with the request")
	ErrTooManySess   = errors.New("too many upload sessions for this server")
	ErrBusy          = errors.New("chunk is already being uploaded or too many chunks are in flight")
	ErrBadChunk      = errors.New("invalid chunk")
	ErrChecksum      = errors.New("checksum mismatch")
	ErrIncomplete    = errors.New("upload is not complete")
	ErrCorrupt       = errors.New("uploaded data does not match the session state")
	ErrExists        = errors.New("a file with that name already exists")
	ErrShortBody     = errors.New("request body ended before the whole chunk was received")
	ErrDiskSpace     = errors.New("not enough disk space")
	ErrInvalidParams = errors.New("invalid upload session parameters")
	ErrTooLarge      = errors.New("file is larger than the maximum upload size")
	ErrIgnored       = errors.New("path is not allowed")
)

// IncompleteError is returned by Complete when chunks are still missing. It
// lists them so the client can send exactly those.
type IncompleteError struct {
	// Missing holds inclusive chunk index ranges, e.g. [[0,2],[7,7]].
	Missing [][2]int
}

func (e *IncompleteError) Error() string {
	return fmt.Sprintf("%v: missing chunks %v", ErrIncomplete, e.Missing)
}

func (e *IncompleteError) Unwrap() error { return ErrIncomplete }

// File is an open file belonging to a server.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	// Sync flushes written data to stable storage.
	Sync() error
	// Size reports the current size of the file.
	Size() (int64, error)
}

// FS is the subset of a server's filesystem that sessions need. It is an
// interface so the session logic can be tested without a real Wings server.
// Implementations MUST confine every path to the server's own root.
type FS interface {
	// HasSpaceFor reports ErrDiskSpace if size more bytes do not fit the server's quota.
	HasSpaceFor(size int64) error
	// IsIgnored reports an error for paths the server's denylist forbids.
	IsIgnored(paths ...string) error
	// OpenPart opens the part file for reading and writing, creating it (and its
	// directory) when needed and never truncating it. Growth is quota-accounted and
	// fails with an error for which IsDiskSpace is true when it would exceed the quota.
	OpenPart(p string) (File, error)
	// StatPart reports the size of the part file, or exists=false.
	StatPart(p string) (size int64, exists bool, err error)
	// Remove deletes a file, freeing its quota. Removing a missing file is not an error.
	Remove(p string) error
	// Replace moves part to final. If final exists it is atomically replaced when replace
	// is true, otherwise ErrExists is returned and both files are left untouched.
	Replace(part, final string, replace bool) error
	// IsDiskSpace reports whether err means the quota was exceeded.
	IsDiskSpace(err error) bool
}

var (
	sessionIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
	serverUUIDPatter = regexp.MustCompile(`^[a-zA-Z0-9-]{8,64}$`)
)

// Params identify a session. They come from the panel-signed token, never from
// the request body, so a client cannot widen them.
type Params struct {
	ServerUUID string
	UserUUID   string
	SessionID  string
	Directory  string
	Name       string
	Size       int64
	// Replace allows atomically replacing an existing file with the same name.
	Replace bool
}

// Validate checks the parameters are safe to use to build paths.
func (p Params) Validate() error {
	switch {
	case !serverUUIDPatter.MatchString(p.ServerUUID):
		return fmt.Errorf("%w: server uuid", ErrInvalidParams)
	case p.UserUUID == "":
		return fmt.Errorf("%w: user uuid", ErrInvalidParams)
	case !sessionIDPattern.MatchString(p.SessionID):
		return fmt.Errorf("%w: session id", ErrInvalidParams)
	case p.Size < 0:
		return fmt.Errorf("%w: size", ErrInvalidParams)
	case p.Name == "" || p.Name == "." || p.Name == ".." || len(p.Name) > 255 ||
		strings.ContainsAny(p.Name, "/\\\x00"):
		return fmt.Errorf("%w: file name", ErrInvalidParams)
	case strings.ContainsRune(p.Directory, 0):
		return fmt.Errorf("%w: directory", ErrInvalidParams)
	}
	return nil
}

// dir returns the cleaned, absolute (within the server root) target directory.
func (p Params) dir() string { return path.Clean("/" + p.Directory) }

// FinalPath is where the finished file ends up.
func (p Params) FinalPath() string { return path.Join(p.dir(), p.Name) }

// PartPath is the hidden file the data is written to meanwhile. Its name does not
// depend on the target's name, so it can never exceed the 255 byte limit.
func (p Params) PartPath() string { return path.Join(p.dir(), ".upload-"+p.SessionID+".part") }

// Limits configure the manager.
type Limits struct {
	ChunkSize            int64
	MaxSessionsPerServer int
	MaxParallelChunks    int
	// MaxSize is the largest accepted file in bytes; 0 means unlimited.
	MaxSize int64
	// SessionTTL is how long a session may stay idle before its data is removed.
	SessionTTL time.Duration
	// CompletedTTL is how long the result of a finished session is remembered, so a
	// "complete" request whose response was lost can be repeated safely.
	CompletedTTL time.Duration
}

// WithDefaults fills zero values.
func (l Limits) WithDefaults() Limits {
	if l.ChunkSize <= 0 {
		l.ChunkSize = DefaultChunkSize
	}
	if l.MaxSessionsPerServer <= 0 {
		l.MaxSessionsPerServer = 4
	}
	if l.MaxParallelChunks <= 0 {
		l.MaxParallelChunks = 8
	}
	if l.SessionTTL <= 0 {
		l.SessionTTL = 24 * time.Hour
	}
	if l.CompletedTTL <= 0 {
		l.CompletedTTL = time.Hour
	}
	return l
}

// Result is the outcome of a completed upload.
type Result struct {
	Name        string    `json:"name"`
	Directory   string    `json:"directory"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	CompletedAt time.Time `json:"completed_at"`
}

// Info is a snapshot of a session, sent to the client so it can resume.
type Info struct {
	SessionID     string    `json:"session_id"`
	Name          string    `json:"name"`
	Directory     string    `json:"directory"`
	Size          int64     `json:"size"`
	ChunkSize     int64     `json:"chunk_size"`
	ChunkCount    int       `json:"chunk_count"`
	MaxParallel   int       `json:"max_parallel"`
	Received      [][2]int  `json:"received"`
	ReceivedBytes int64     `json:"received_bytes"`
	HashedBytes   int64     `json:"hashed_bytes"`
	Completed     *Result   `json:"completed,omitempty"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// ChunkResult is the outcome of a chunk upload.
type ChunkResult struct {
	Index   int    `json:"index"`
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
	Already bool   `json:"already"`
}

// compress turns a sorted list of indexes into inclusive [from,to] ranges.
func compress(sorted []int) [][2]int {
	out := make([][2]int, 0, 4)
	for _, i := range sorted {
		if n := len(out); n > 0 && out[n-1][1] == i-1 {
			out[n-1][1] = i
			continue
		}
		out = append(out, [2]int{i, i})
	}
	return out
}
