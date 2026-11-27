package filesystem

import (
	"os"
	"path/filepath"

	"emperror.dev/errors"
	"golang.org/x/sys/unix"

	"github.com/pterodactyl/wings/internal/ufs"
)

// EnsureDirectory creates p and any missing parents, chowning every directory it
// creates to the server user, exactly like the legacy upload does.
func (fs *Filesystem) EnsureDirectory(p string) error {
	return fs.mkdirAll(p, 0o755)
}

// OpenForChunkedWrite opens p for reading and writing without truncating it,
// creating it (and its parent directories) if needed. Writes through the
// returned file are accounted against the server's disk limit: growing the file
// past the limit fails with an E_NODISK error and nothing is written.
//
// Callers that write several chunks at different offsets must share ONE returned
// file between them. The quota accounting tracks the highest offset written
// through a given file, so separate handles to the same path would each reserve
// the same range again.
func (fs *Filesystem) OpenForChunkedWrite(p string) (ufs.File, error) {
	if err := fs.checkNotDirectory(p); err != nil {
		return nil, err
	}
	if err := fs.mkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}

	return fs.Touch(p, ufs.O_RDWR)
}

// SyncFile flushes the file's data (not necessarily its metadata) to stable
// storage.
func SyncFile(f ufs.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

// ErrReplaceTarget is returned by ReplaceFile when the destination exists and
// either may not be replaced or is not a regular file.
var ErrReplaceTarget = errors.New("filesystem: destination exists and cannot be replaced")

// ReplaceFile moves the regular file oldp to newp.
//
// If newp does not exist the move cannot overwrite anything that appears there in
// the meantime. If newp exists it is replaced atomically, but only when replace
// is true and it is a regular file; otherwise ErrReplaceTarget is returned and
// both files are left untouched. The replaced file's permission bits are kept,
// and the moved file is chowned to the server user first so it is never visible
// with the wrong owner.
func (fs *Filesystem) ReplaceFile(oldp, newp string, replace bool) error {
	existing, statErr := fs.unixFS.Lstat(newp)
	switch {
	case statErr == nil:
		if !replace || !existing.Mode().IsRegular() {
			return ErrReplaceTarget
		}
	case !errors.Is(statErr, os.ErrNotExist):
		return statErr
	}

	if err := fs.chownFile(oldp); err != nil {
		return err
	}

	if statErr != nil {
		// Nothing there: a plain rename refuses to overwrite, which also closes the
		// race with a file being created at newp right now.
		if err := fs.unixFS.Rename(oldp, newp); err != nil {
			if errors.Is(err, os.ErrExist) {
				return ErrReplaceTarget
			}
			return err
		}
		return nil
	}

	// Keep the permissions the file being replaced had (an uploaded file would
	// otherwise reset an executable bit, for example).
	_ = fs.unixFS.Chmod(oldp, existing.Mode().Perm())

	if err := fs.unixFS.RenameReplace(oldp, newp); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrReplaceTarget
		}
		return err
	}
	return nil
}
