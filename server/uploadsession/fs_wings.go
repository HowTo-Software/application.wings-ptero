//go:build unix

package uploadsession

import (
	"errors"
	"fmt"
	"os"

	"github.com/pterodactyl/wings/internal/ufs"
	"github.com/pterodactyl/wings/server/filesystem"
)

// WingsFS adapts a server's real, rooted filesystem to the FS interface. Every
// path is resolved through the server's own filesystem (openat2-style
// containment), so a session can never reach outside its server's volume, and
// writes go through the same quota accounting as every other write.
func WingsFS(fs *filesystem.Filesystem) FS { return wingsFS{fs: fs} }

type wingsFS struct{ fs *filesystem.Filesystem }

type wingsFile struct{ ufs.File }

func (f wingsFile) Sync() error { return filesystem.SyncFile(f.File) }

func (f wingsFile) Size() (int64, error) {
	st, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (w wingsFS) HasSpaceFor(size int64) error {
	if err := w.fs.HasSpaceFor(size); err != nil {
		return fmt.Errorf("%w: %v", ErrDiskSpace, err)
	}
	return nil
}

func (w wingsFS) IsIgnored(paths ...string) error { return w.fs.IsIgnored(paths...) }

func (w wingsFS) OpenPart(p string) (File, error) {
	f, err := w.fs.OpenForChunkedWrite(p)
	if err != nil {
		return nil, err
	}
	return wingsFile{f}, nil
}

func (w wingsFS) StatPart(p string) (int64, bool, error) {
	st, err := w.fs.UnixFS().Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !st.Mode().IsRegular() {
		return 0, false, nil
	}
	return st.Size(), true, nil
}

func (w wingsFS) Remove(p string) error { return w.fs.Delete(p) }

func (w wingsFS) Replace(part, final string, replace bool) error {
	if err := w.fs.ReplaceFile(part, final, replace); err != nil {
		if errors.Is(err, filesystem.ErrReplaceTarget) {
			return ErrExists
		}
		return err
	}
	return nil
}

func (w wingsFS) IsDiskSpace(err error) bool {
	return filesystem.IsErrorCode(err, filesystem.ErrCodeDiskSpace) || errors.Is(err, ErrDiskSpace)
}
