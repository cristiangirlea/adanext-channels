package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Manifest struct {
	Name      string   `json:"name"`
	Label     string   `json:"label"`
	SecretEnv string   `json:"secret_env"`
	Hosts     []string `json:"hosts"`
}

var (
	manifestName = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)
	envName      = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	hostLabel    = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)
)

func (m Manifest) Validate() error {
	if !manifestName.MatchString(m.Name) {
		return errors.New("manifest: invalid name")
	}
	if !utf8.ValidString(m.Label) || len(m.Label) > 128 || strings.IndexFunc(m.Label, unicode.IsControl) >= 0 {
		return errors.New("manifest: invalid label")
	}
	if !envName.MatchString(m.SecretEnv) {
		return errors.New("manifest: invalid secret_env")
	}
	if len(m.Hosts) == 0 {
		return errors.New("manifest: hosts is empty")
	}
	for _, host := range m.Hosts {
		if len(host) > 253 {
			return errors.New("manifest: hostname exceeds 253 bytes")
		}
		for label := range strings.SplitSeq(strings.TrimPrefix(host, "*."), ".") {
			if !hostLabel.MatchString(label) {
				return fmt.Errorf("manifest: invalid hostname %q", host)
			}
		}
	}
	return nil
}

func LoadManifest(path string) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()
	m, err := decodeManifest(json.NewDecoder(f))
	if err != nil {
		return Manifest{}, fmt.Errorf("load manifest %q: %w", path, err)
	}
	return m, nil
}

func decodeManifest(dec *json.Decoder) (Manifest, error) {
	var m Manifest
	token, err := dec.Token()
	if err != nil {
		return m, err
	}
	if token != json.Delim('{') {
		return m, errors.New("expected object")
	}
	// Decoding keys explicitly keeps secret and host declarations case-sensitive and unique.
	fields := map[string]any{"name": &m.Name, "label": &m.Label, "secret_env": &m.SecretEnv, "hosts": &m.Hosts}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return m, err
		}
		name, _ := key.(string)
		dst, ok := fields[name]
		if !ok {
			return m, fmt.Errorf("unknown or duplicate field %q", name)
		}
		delete(fields, name)
		if err := dec.Decode(dst); err != nil {
			return m, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return m, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return m, errors.New("unexpected data after manifest")
	}
	return m, m.Validate()
}
