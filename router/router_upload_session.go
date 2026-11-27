package router

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/internal/models"
	"github.com/pterodactyl/wings/router/middleware"
	"github.com/pterodactyl/wings/router/tokens"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/uploadsession"
)

// Chunked, resumable uploads (HowTo.Software extension). See package uploadsession.
//
// Every route is authorized by a Panel-signed JWT passed as ?token=, exactly like
// the legacy POST /upload/file, and none of them changes that endpoint.

var (
	uploadSessionsMu  sync.Mutex
	uploadSessionsMgr *uploadsession.Manager
)

// uploadSessionLimits builds the manager limits from the current configuration.
func uploadSessionLimits() uploadsession.Limits {
	c := config.Get().Api.UploadSessions
	return uploadsession.Limits{
		ChunkSize:            c.ChunkSize,
		MaxParallelChunks:    c.MaxParallelChunks,
		MaxSessionsPerServer: c.MaxSessionsPerServer,
		SessionTTL:           time.Duration(c.SessionTTLHours) * time.Hour,
	}
}

// getUploadSessions returns the process-wide manager, creating it on first use.
// Session state lives next to Wings' own state, never inside a server's volume.
func getUploadSessions() *uploadsession.Manager {
	uploadSessionsMu.Lock()
	defer uploadSessionsMu.Unlock()

	root := filepath.Join(config.Get().System.RootDirectory, "upload-sessions")
	if uploadSessionsMgr == nil || uploadSessionsRoot != root {
		if uploadSessionsMgr != nil {
			uploadSessionsMgr.Close()
		}
		uploadSessionsMgr = uploadsession.NewManager(root, uploadSessionLimits())
		uploadSessionsRoot = root
	}
	return uploadSessionsMgr
}

var uploadSessionsRoot string

// RunUploadSessionSweeper periodically removes expired sessions (and the partial
// files they hold, freeing the servers' disk quota) until ctx is done. It also
// closes the open file handles when it stops.
func RunUploadSessionSweeper(ctx context.Context, m *server.Manager) {
	if !config.Get().Api.UploadSessions.Enabled {
		return
	}
	go func() {
		mgr := getUploadSessions()
		defer mgr.Close()

		resolve := func(uuid string) (uploadsession.FS, bool) {
			s, ok := m.Get(uuid)
			if !ok {
				return nil, false
			}
			return uploadsession.WingsFS(s.Filesystem()), true
		}

		sweep := func() {
			if n := mgr.Sweep(resolve); n > 0 {
				log.WithField("removed", n).Info("removed expired upload sessions")
			}
		}
		sweep()

		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sweep()
			}
		}
	}()
}

// uploadCapabilities describes the feature for GET /api/system?v=2, which is how
// the Panel learns whether a node runs this fork. The key is namespaced so it can
// never collide with an upstream field.
func uploadCapabilities() map[string]any {
	cfg := config.Get().Api
	if !cfg.UploadSessions.Enabled {
		return nil
	}
	l := uploadSessionLimits().WithDefaults()
	return map[string]any{
		"howto_upload_sessions": map[string]any{
			"version":             uploadsession.Version,
			"chunk_size":          l.ChunkSize,
			"max_parallel_chunks": l.MaxParallelChunks,
			"max_size":            cfg.UploadLimit * 1024 * 1024,
			"hash":                "sha256",
			"http2":               !cfg.UploadSessions.DisableHTTP2,
		},
	}
}

func uploadError(c *gin.Context, status int, code, message string, extra ...gin.H) {
	body := gin.H{"error": message, "code": code}
	for _, e := range extra {
		for k, v := range e {
			body[k] = v
		}
	}
	c.AbortWithStatusJSON(status, body)
}

// authorizeUploadSession validates the token and resolves the server. It writes the
// error response itself and returns ok=false when the request must stop.
func authorizeUploadSession(c *gin.Context, requireSessionParam bool) (*server.Server, *tokens.UploadSessionPayload, bool) {
	if !config.Get().Api.UploadSessions.Enabled {
		uploadError(c, http.StatusNotFound, "E_DISABLED", "Chunked uploads are not enabled on this node.")
		return nil, nil, false
	}

	var payload tokens.UploadSessionPayload
	if err := tokens.ParseToken([]byte(c.Query("token")), &payload); err != nil {
		// 401 tells the client to ask the Panel for a fresh token and retry.
		uploadError(c, http.StatusUnauthorized, "E_TOKEN", "The upload token is invalid or has expired.")
		return nil, nil, false
	}
	s, ok := middleware.ExtractManager(c).Get(payload.ServerUuid)
	if !ok {
		uploadError(c, http.StatusNotFound, "E_NO_SERVER", "The requested resource was not found on this server.")
		return nil, nil, false
	}
	if payload.Denylisted() || !payload.HasScope(tokens.UploadSession) {
		uploadError(c, http.StatusForbidden, "E_REVOKED", "Access to this server was revoked or the token has no upload scope.")
		return nil, nil, false
	}
	if requireSessionParam && c.Param("session") != payload.SessionId {
		uploadError(c, http.StatusForbidden, "E_SESSION", "The token does not belong to this upload session.")
		return nil, nil, false
	}

	// The request is authenticated: lift the short deadlines applied to anonymous requests.
	middleware.ClearDeadlines(c)
	return s, &payload, true
}

func sessionParams(p *tokens.UploadSessionPayload) uploadsession.Params {
	return uploadsession.Params{
		ServerUUID: p.ServerUuid,
		UserUUID:   p.UserUuid,
		SessionID:  p.SessionId,
		Directory:  p.Directory,
		Name:       p.Name,
		Size:       p.Size,
		Replace:    p.Replace,
	}
}

// writeUploadSessionError maps session errors to HTTP responses a client can act on.
func writeUploadSessionError(c *gin.Context, err error) {
	var incomplete *uploadsession.IncompleteError

	switch {
	case errors.As(err, &incomplete):
		uploadError(c, http.StatusConflict, "E_INCOMPLETE", "Some chunks have not been received yet.", gin.H{"missing": incomplete.Missing})
	case errors.Is(err, uploadsession.ErrNotFound):
		uploadError(c, http.StatusNotFound, "E_NOT_FOUND", "Upload session not found.")
	case errors.Is(err, uploadsession.ErrConflict):
		uploadError(c, http.StatusConflict, "E_CONFLICT", err.Error())
	case errors.Is(err, uploadsession.ErrExists):
		uploadError(c, http.StatusConflict, "E_EXISTS", "A file with that name already exists and replacing it is not allowed.")
	case errors.Is(err, uploadsession.ErrCorrupt):
		uploadError(c, http.StatusConflict, "E_CORRUPT", err.Error())
	case errors.Is(err, uploadsession.ErrBusy):
		c.Header("Retry-After", "1")
		uploadError(c, http.StatusTooManyRequests, "E_BUSY", "Too many chunks are being uploaded at once; retry shortly.")
	case errors.Is(err, uploadsession.ErrTooManySess):
		uploadError(c, http.StatusTooManyRequests, "E_TOO_MANY_SESSIONS", "Too many unfinished uploads for this server; finish or cancel one first.")
	case errors.Is(err, uploadsession.ErrChecksum):
		uploadError(c, http.StatusUnprocessableEntity, "E_CHECKSUM", err.Error())
	case errors.Is(err, uploadsession.ErrDiskSpace):
		uploadError(c, http.StatusInsufficientStorage, "E_NODISK", "There is not enough disk space on this server for the upload.")
	case errors.Is(err, uploadsession.ErrTooLarge):
		uploadError(c, http.StatusBadRequest, "E_TOO_LARGE", "The file is larger than the maximum upload size of this node.")
	case errors.Is(err, uploadsession.ErrIgnored):
		uploadError(c, http.StatusForbidden, "E_IGNORED", "That path is not allowed for this server.")
	case errors.Is(err, uploadsession.ErrBadChunk), errors.Is(err, uploadsession.ErrInvalidParams):
		uploadError(c, http.StatusBadRequest, "E_BAD_REQUEST", err.Error())
	case errors.Is(err, uploadsession.ErrShortBody),
		errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, context.Canceled),
		c.Request.Context().Err() != nil:
		// The client went away mid-chunk. Nothing is recorded for the chunk; it is simply sent again.
		uploadError(c, http.StatusBadRequest, "E_SHORT_BODY", "The request body ended before the whole chunk arrived.")
	default:
		middleware.CaptureAndAbort(c, err)
	}
}

// postUploadSessionCreate creates a session, or returns the existing one so the
// client can resume it (same file => same session id).
func postUploadSessionCreate(c *gin.Context) {
	s, payload, ok := authorizeUploadSession(c, false)
	if !ok {
		return
	}

	// Same node-wide limit as the legacy endpoint, enforced before accepting any data.
	if limit := config.Get().Api.UploadLimit; limit > 0 && payload.Size > limit*1024*1024 {
		uploadError(c, http.StatusBadRequest, "E_TOO_LARGE",
			"File "+payload.Name+" is larger than the maximum file upload size of "+strconv.FormatInt(limit, 10)+" MB.")
		return
	}

	sess, created, err := getUploadSessions().CreateOrResume(uploadsession.WingsFS(s.Filesystem()), sessionParams(payload))
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.JSON(status, gin.H{"resumed": !created, "session": sess.Info()})
}

func getUploadSession(c *gin.Context) {
	s, payload, ok := authorizeUploadSession(c, true)
	if !ok {
		return
	}
	sess, err := getUploadSessions().Get(uploadsession.WingsFS(s.Filesystem()), payload.ServerUuid, payload.UserUuid, payload.SessionId)
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"session": sess.Info()})
}

func putUploadSessionChunk(c *gin.Context) {
	s, payload, ok := authorizeUploadSession(c, true)
	if !ok {
		return
	}
	index, err := strconv.Atoi(c.Param("index"))
	if err != nil {
		uploadError(c, http.StatusBadRequest, "E_BAD_REQUEST", "The chunk index must be a number.")
		return
	}
	if c.Request.ContentLength < 0 {
		uploadError(c, http.StatusLengthRequired, "E_LENGTH", "Missing Content-Length.")
		return
	}

	sess, err := getUploadSessions().Get(uploadsession.WingsFS(s.Filesystem()), payload.ServerUuid, payload.UserUuid, payload.SessionId)
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}
	res, err := sess.PutChunk(index, c.Request.Body, c.Request.ContentLength, c.Query("sha256"))
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}
	c.JSON(http.StatusOK, res)
}

func postUploadSessionComplete(c *gin.Context) {
	s, payload, ok := authorizeUploadSession(c, true)
	if !ok {
		return
	}
	sess, err := getUploadSessions().Get(uploadsession.WingsFS(s.Filesystem()), payload.ServerUuid, payload.UserUuid, payload.SessionId)
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}

	var body struct {
		SHA256 string `json:"sha256"`
	}
	if c.Request.ContentLength != 0 {
		if err := c.ShouldBindJSON(&body); err != nil && !errors.Is(err, io.EOF) {
			uploadError(c, http.StatusBadRequest, "E_BAD_REQUEST", "The request body must be JSON.")
			return
		}
	}

	alreadyDone := sess.Info().Completed != nil
	res, err := sess.Complete(body.SHA256)
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}

	if !alreadyDone {
		s.SaveActivity(s.NewRequestActivity(payload.UserUuid, c.ClientIP()), server.ActivityFileUploaded, models.ActivityMeta{
			"file":      res.Name,
			"directory": path.Clean(res.Directory),
		})
	}
	c.JSON(http.StatusOK, res)
}

func deleteUploadSession(c *gin.Context) {
	s, payload, ok := authorizeUploadSession(c, true)
	if !ok {
		return
	}
	sess, err := getUploadSessions().Get(uploadsession.WingsFS(s.Filesystem()), payload.ServerUuid, payload.UserUuid, payload.SessionId)
	if err != nil {
		writeUploadSessionError(c, err)
		return
	}
	if err := sess.Abort(); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// registerUploadSessionRoutes adds the routes. They live beside, and never replace, /upload/file.
func registerUploadSessionRoutes(r gin.IRoutes) {
	r.POST("/upload/session", postUploadSessionCreate)
	r.GET("/upload/session/:session", getUploadSession)
	r.PUT("/upload/session/:session/chunks/:index", putUploadSessionChunk)
	r.POST("/upload/session/:session/complete", postUploadSessionComplete)
	r.DELETE("/upload/session/:session", deleteUploadSession)
}
