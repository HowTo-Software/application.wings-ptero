package tokens

import (
	"github.com/gbrlsnchs/jwt/v3"
)

// UploadSessionPayload authorizes every request of one chunked upload session.
//
// Unlike UploadPayload it is NOT single-use: a session needs many requests. It is
// instead bound to exactly one session, server, user, directory, file name and
// size, all signed by the Panel, so the bearer cannot send data anywhere else.
// It is subject to the same expiry and revocation (Denylisted) rules as every
// other token; the client asks the Panel for a fresh one when it expires.
type UploadSessionPayload struct {
	jwt.Payload
	Scoped

	ServerUuid string `json:"server_uuid"`
	UserUuid   string `json:"user_uuid"`
	SessionId  string `json:"session_id"`
	Directory  string `json:"directory"`
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	// Replace is true when the user may replace an existing file with the same name.
	Replace bool `json:"replace"`
}

// GetPayload returns the JWT payload.
func (p *UploadSessionPayload) GetPayload() *jwt.Payload {
	return &p.Payload
}

// Denylisted returns true if this token was issued before the user's access to
// the server was revoked.
func (p *UploadSessionPayload) Denylisted() bool {
	return isDenylisted(&p.Payload, p.ServerUuid, p.UserUuid)
}
