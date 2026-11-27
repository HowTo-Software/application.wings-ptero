package uploadsession

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Manager owns the sessions of a Wings instance.
//
// Lock order: a Session's mutex may be held while taking the Manager's mutex
// (Session.Abort -> forget), never the other way around.
type Manager struct {
	root   string // directory holding <server uuid>/<session id>.json|.chunks
	limits Limits

	mu       sync.Mutex
	sessions map[string]*Session
}

// NewManager creates a manager that keeps session state under root. The directory
// must NOT be inside any server's volume.
func NewManager(root string, limits Limits) *Manager {
	return &Manager{root: root, limits: limits.WithDefaults(), sessions: make(map[string]*Session)}
}

// Limits returns the effective limits.
func (m *Manager) Limits() Limits { return m.limits }

func key(serverUUID, id string) string { return serverUUID + "/" + id }

func (m *Manager) serverDir(serverUUID string) string { return filepath.Join(m.root, serverUUID) }

func (m *Manager) forget(s *Session) {
	m.mu.Lock()
	delete(m.sessions, key(s.p.ServerUUID, s.p.SessionID))
	m.mu.Unlock()
}

// CreateOrResume returns the session for p, creating it when it does not exist.
//
// The same parameters always map to the same session, so a client that reconnects
// (or reopens the browser and picks the same file again) simply resumes. A session
// can only be resumed by the user that created it and only with identical size,
// directory and name.
func (m *Manager) CreateOrResume(fs FS, p Params) (*Session, bool, error) {
	if err := p.Validate(); err != nil {
		return nil, false, err
	}
	if m.limits.MaxSize > 0 && p.Size > m.limits.MaxSize {
		return nil, false, ErrTooLarge
	}

	s, err := m.load(fs, p.ServerUUID, p.SessionID)
	if err == nil && s.Info().Completed != nil {
		// A finished session only lives on as a tombstone so a repeated "complete" can be answered. Asking to
		// create the same upload again (same file picked once more) means a NEW upload that replaces the result.
		if merr := matches(s, p); merr != nil {
			return nil, false, merr
		}
		_ = s.Abort()
		err = ErrNotFound
	}
	switch {
	case err == nil:
		if err := matches(s, p); err != nil {
			return nil, false, err
		}
		// The token is fresh, so its permission to replace is too.
		s.mu.Lock()
		s.p.Replace = p.Replace
		s.touchLocked()
		_ = s.persistLocked()
		s.mu.Unlock()
		return s, false, nil
	case !errors.Is(err, ErrNotFound):
		return nil, false, err
	}

	// New session: refuse anything that could not succeed before accepting a byte.
	if err := fs.IsIgnored(p.FinalPath(), p.PartPath()); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrIgnored, err)
	}
	if err := fs.HasSpaceFor(p.Size); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrDiskSpace, err)
	}
	if m.countServer(p.ServerUUID) >= m.limits.MaxSessionsPerServer {
		return nil, false, ErrTooManySess
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(p.ServerUUID, p.SessionID)
	if existing, ok := m.sessions[k]; ok { // lost a race with a concurrent create
		return existing, false, matches(existing, p)
	}
	s = newSession(m, fs, p, m.limits.ChunkSize, m.serverDir(p.ServerUUID))
	s.mu.Lock()
	err = s.persistLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	m.sessions[k] = s
	return s, true, nil
}

// Get returns an existing session, restoring it from disk after a restart. Sessions
// of other servers or other users are reported as not found.
func (m *Manager) Get(fs FS, serverUUID, userUUID, id string) (*Session, error) {
	if !serverUUIDPatter.MatchString(serverUUID) || !sessionIDPattern.MatchString(id) {
		return nil, ErrNotFound
	}
	s, err := m.load(fs, serverUUID, id)
	if err != nil {
		return nil, err
	}
	if s.p.UserUUID != userUUID {
		return nil, ErrNotFound
	}
	s.mu.Lock()
	s.touchLocked()
	s.mu.Unlock()
	return s, nil
}

func matches(s *Session, p Params) error {
	cur := s.Params()
	switch {
	case cur.UserUUID != p.UserUUID:
		return ErrNotFound // do not reveal that another user's session exists
	case cur.Size != p.Size || cur.dir() != p.dir() || cur.Name != p.Name:
		return fmt.Errorf("%w: session %s already exists for a different file", ErrConflict, p.SessionID)
	}
	return nil
}

func (m *Manager) load(fs FS, serverUUID, id string) (*Session, error) {
	k := key(serverUUID, id)

	m.mu.Lock()
	if s, ok := m.sessions[k]; ok {
		m.mu.Unlock()
		return s, nil
	}
	m.mu.Unlock()

	s, err := loadSession(m, fs, serverUUID, id, m.serverDir(serverUUID))
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.sessions[k]; ok {
		return existing, nil
	}
	m.sessions[k] = s
	return s, nil
}

// countServer counts the unfinished sessions a server has, loaded or not.
func (m *Manager) countServer(serverUUID string) int {
	entries, err := os.ReadDir(m.serverDir(serverUUID))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		m.mu.Lock()
		s, ok := m.sessions[key(serverUUID, id)]
		m.mu.Unlock()
		if ok {
			if s.Info().Completed != nil {
				continue
			}
		} else if d, err := readPersisted(filepath.Join(m.serverDir(serverUUID), e.Name())); err == nil && d.Completed != nil {
			continue
		}
		n++
	}
	return n
}

// Resolver finds the filesystem of a server, or false when the server no longer
// exists on this node.
type Resolver func(serverUUID string) (FS, bool)

// Sweep removes what is no longer needed: sessions idle for longer than the TTL
// (together with their part files, freeing the quota), finished sessions whose
// tombstone expired, and state of servers that were deleted. It also closes the
// file handles of idle sessions. It is safe to call concurrently with uploads.
func (m *Manager) Sweep(resolve Resolver) (removed int) {
	servers, err := os.ReadDir(m.root)
	if err != nil {
		return 0
	}
	now := time.Now().UTC()

	for _, srv := range servers {
		if !srv.IsDir() {
			continue
		}
		serverUUID := srv.Name()
		fs, serverExists := resolve(serverUUID)
		dir := m.serverDir(serverUUID)
		entries, _ := os.ReadDir(dir)

		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".json") {
				// A leftover temporary file or a chunk log without a session.
				if strings.HasSuffix(name, ".tmp") || (strings.HasSuffix(name, ".chunks") && !fileExists(filepath.Join(dir, strings.TrimSuffix(name, ".chunks")+".json"))) {
					_ = os.Remove(filepath.Join(dir, name))
				}
				continue
			}
			id := strings.TrimSuffix(name, ".json")
			d, err := readPersisted(filepath.Join(dir, name))
			if err != nil {
				_ = os.Remove(filepath.Join(dir, name))
				_ = os.Remove(filepath.Join(dir, id+".chunks"))
				removed++
				continue
			}

			switch {
			case !serverExists:
				_ = os.Remove(filepath.Join(dir, name))
				_ = os.Remove(filepath.Join(dir, id+".chunks"))
				m.mu.Lock()
				delete(m.sessions, key(serverUUID, id))
				m.mu.Unlock()
				removed++
			case d.Completed != nil && now.Sub(d.Completed.CompletedAt) > m.limits.CompletedTTL,
				d.Completed == nil && now.Sub(d.UpdatedAt) > m.limits.SessionTTL:
				if s, err := m.load(fs, serverUUID, id); err == nil {
					if s.hasInflight() {
						continue
					}
					_ = s.Abort()
				} else {
					_ = os.Remove(filepath.Join(dir, name))
					_ = os.Remove(filepath.Join(dir, id+".chunks"))
				}
				removed++
			default:
				m.mu.Lock()
				s := m.sessions[key(serverUUID, id)]
				m.mu.Unlock()
				if s != nil {
					s.releaseIdle(2 * time.Minute)
				}
			}
		}
		if serverExists || len(entries) == 0 {
			// Drop empty per-server directories.
			_ = os.Remove(dir)
		} else {
			_ = os.Remove(dir)
		}
	}
	return removed
}

func (s *Session) hasInflight() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight) > 0
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// Close releases every open part file handle. Session state stays on disk, so
// uploads can be resumed after the process starts again.
func (m *Manager) Close() {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()

	for _, s := range sessions {
		s.mu.Lock()
		s.closeFileLocked()
		s.mu.Unlock()
	}
}
