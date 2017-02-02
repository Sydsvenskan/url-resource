package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/Sydsvenskan/concourse"
	"github.com/pkg/errors"
)

func main() {
	context, err := concourse.NewContext(os.Args, os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to create command context:", err.Error())
		os.Exit(1)
		return
	}

	context.Handle(&concourse.Resource{
		Check: &CheckCommand{},
		In:    &InCommand{},
		Out:   &OutCommand{},
	})
}

// CheckCommand check-command payload
type CheckCommand struct {
	// Source definition
	Source Source `json:"source"`
	// Version information passed to the resource
	Version concourse.ResourceVersion `json:"version"`
}

// HandleCommand runs the command
func (cmd *CheckCommand) HandleCommand(ctx *concourse.CommandContext) (
	*concourse.CommandResponse, error,
) {
	var err error
	var resp concourse.CommandResponse

	if cmd.Version != nil {
		resp.Versions = append(resp.Versions, cmd.Version)
	}

	etag := cmd.Version["etag"]
	hash := cmd.Version["sha1"]

	timeout := 5 * time.Minute
	if cmd.Source.Timeout != "" {
		timeout, err = time.ParseDuration(cmd.Source.Timeout)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse timeout")
		}
	}

	client := http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequest("GET", cmd.Source.URL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	// Add source headers
	for name, values := range cmd.Source.Headers {
		req.Header[name] = append(req.Header[name], values...)
	}

	if etag != "" {
		req.Header.Add("If-None-Match", etag)
	}

	if cmd.Source.BasicAuth != nil {
		req.SetBasicAuth(
			cmd.Source.BasicAuth.User,
			cmd.Source.BasicAuth.Password,
		)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to perform request")
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotModified {
		return &resp, nil
	}

	version := concourse.ResourceVersion{}
	responseETag := res.Header.Get("ETag")
	if responseETag != "" {
		// Catch cases where an identical Etag is returned but the
		// server responded with a 200 OK anyway.
		if responseETag == etag {
			return &resp, nil
		}

		version["etag"] = responseETag

	} else {
		h := sha1.New()
		_, err := io.Copy(h, res.Body)
		if err != nil {
			return nil, errors.Wrap(err, "failed to hash response contents")
		}
		version["sha1"] = fmt.Sprintf("%x", h.Sum(nil))

		if version["sha1"] == hash {
			return &resp, nil
		}
	}

	// Prepend the new version to the versions array
	resp.Versions = []concourse.ResourceVersion{
		version,
	}

	return &resp, nil
}

type Source struct {
	URL       string      `json:"url"`
	Timeout   string      `json:"timeout"`
	Headers   http.Header `json:"headers,omitempty"`
	BasicAuth *BasicAuth  `json:"basic_auth,omitempty"`
}

type BasicAuth struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// InCommand in-command payload
type InCommand struct {
	// Source definition
	Source Source `json:"source"`
	// Version is used in the implicit post `put` `get`
	Version concourse.ResourceVersion
}

// HandleCommand runs the command
func (cmd *InCommand) HandleCommand(ctx *concourse.CommandContext) (
	*concourse.CommandResponse, error,
) {
	var err error
	var resp concourse.CommandResponse

	etag := cmd.Version["etag"]
	hash := cmd.Version["sha1"]

	timeout := 5 * time.Minute
	if cmd.Source.Timeout != "" {
		timeout, err = time.ParseDuration(cmd.Source.Timeout)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse timeout")
		}
	}

	client := http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequest("GET", cmd.Source.URL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	// Add source headers
	for name, values := range cmd.Source.Headers {
		req.Header[name] = append(req.Header[name], values...)
	}

	if cmd.Source.BasicAuth != nil {
		req.SetBasicAuth(
			cmd.Source.BasicAuth.User,
			cmd.Source.BasicAuth.Password,
		)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to perform request")
	}
	defer res.Body.Close()

	version := concourse.ResourceVersion{}
	responseETag := res.Header.Get("ETag")
	if etag != "" && etag != responseETag {
		return nil, errors.Errorf(
			"unexpected ETag %q, expected %q", responseETag, etag,
		)
	}
	if responseETag != "" {
		version["etag"] = responseETag
	}
	output, err := os.Create(filepath.Join(ctx.Directory(), "downloaded"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file for the download")
	}
	tee := io.TeeReader(res.Body, output)

	h := sha1.New()
	_, err = io.Copy(h, tee)
	if err != nil {
		return nil, errors.Wrap(err, "failed to write out download")
	}
	version["sha1"] = fmt.Sprintf("%x", h.Sum(nil))

	if hash != "" && version["sha1"] != hash {
		return nil, errors.Errorf("unexpected SHA1 content hash %q, expected %q",
			version["sha1"], hash,
		)
	}

	resp.Version = version
	resp.AddMeta("content-type", res.Header.Get("Content-type"))

	return &resp, nil
}

// OutCommand in-command payload
type OutCommand struct{}

// HandleCommand runs the command
func (cmd *OutCommand) HandleCommand(ctx *concourse.CommandContext) (
	*concourse.CommandResponse, error,
) {
	return nil, errors.New("not implemented")
}
