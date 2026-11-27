// SPDX-License-Identifier: MIT

//go:build unix

package ufs

import (
	"errors"

	"golang.org/x/sys/unix"
)

// RenameReplace is like Rename except that an existing regular file at newpath
// is atomically replaced: at every moment newpath refers either to the old file
// or to the new one, never to a partially written file or to nothing.
//
// Both paths are resolved with the same containment rules as Rename, so neither
// can escape the base directory. oldpath must be a regular file. If newpath
// exists and is not a regular file (a directory, a symlink, a device, ...) it is
// never replaced and ErrExist is returned.
//
// Unlike Rename the parent directory of newpath must already exist.
func (fs *UnixFS) RenameReplace(oldpath, newpath string) error {
	if oldpath == newpath {
		return nil
	}

	olddirfd, oldname, closeFd, err := fs.safePath(oldpath)
	defer closeFd()
	if err != nil {
		return err
	}
	if oldname == "." {
		return &PathError{Op: "rename", Path: oldname, Err: ErrBadPathResolution}
	}
	oldInfo, err := fs.Lstatat(olddirfd, oldname)
	if err != nil {
		return err
	}
	if !oldInfo.Mode().IsRegular() {
		return &PathError{Op: "rename", Path: oldname, Err: ErrNotRegular}
	}

	newdirfd, newname, closeFd2, err := fs.safePath(newpath)
	defer closeFd2()
	if err != nil {
		return err
	}
	if newname == "." {
		return &PathError{Op: "rename", Path: newname, Err: ErrBadPathResolution}
	}

	switch newInfo, err := fs.Lstatat(newdirfd, newname); {
	case err == nil:
		if !newInfo.Mode().IsRegular() {
			return &PathError{Op: "rename", Path: newname, Err: ErrExist}
		}
	case !errors.Is(err, ErrNotExist):
		return err
	}

	// renameat(2) atomically replaces an existing regular file.
	if err := unix.Renameat(olddirfd, oldname, newdirfd, newname); err != nil {
		return &LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	return nil
}
