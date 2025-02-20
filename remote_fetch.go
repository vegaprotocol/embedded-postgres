package embeddedpostgres

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// RemoteFetchStrategy provides a strategy to fetch a Postgres binary so that it is available for use.
type RemoteFetchStrategy func() error

var (
	supportedOS   = []string{"linux", "darwin"}
	supportedArch = []string{"amd64", "arm64"}
)

func isSupported(value string, supported ...string) bool {
	for _, platform := range supported {
		if strings.HasPrefix(value, platform) {
			return true
		}
	}

	return false
}

//nolint:funlen
func defaultRemoteFetchStrategy(remoteFetchHost, binaryRepoRelease string, versionStrategy VersionStrategy, cacheLocator CacheLocator) RemoteFetchStrategy {
	return func() error {
		operatingSystem, architecture, version := versionStrategy()

		// For now, we're only supporting Linux and MacOS until we have time to build and test the Windows Postgres binaries.
		if !isSupported(operatingSystem, supportedOS...) {
			return fmt.Errorf("unsupported operating system: %s. Currently only Linux and MacOS are supported", operatingSystem)
		}

		// For now, we're not supporting arm architecture until we have time to build and test the arm Postgres binaries.
		if !isSupported(architecture, supportedArch...) {
			return fmt.Errorf("unsupported architecture: %s", architecture)
		}

		// https://github.com/vegaprotocol/embedded-postgres-binaries/releases/download/v0.1.0/embedded-postgres-binaries-darwin-amd64-14.1.0.zip
		downloadURL := fmt.Sprintf("%s/vegaprotocol/embedded-postgres-binaries/releases/download/%s/embedded-postgres-binaries-%s-%s-%s.zip",
			remoteFetchHost,
			binaryRepoRelease,
			operatingSystem,
			architecture,
			version)

		resp, err := http.Get(downloadURL)
		if err != nil {
			return fmt.Errorf("unable to connect to %s", remoteFetchHost)
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("no version found matching %s", version)
		}

		defer func() {
			if err := resp.Body.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		return decompressResponse(resp, cacheLocator, downloadURL)
	}
}

func decompressResponse(resp *http.Response, cacheLocator CacheLocator, downloadURL string) error {
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errorFetchingPostgres(err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(bodyBytes), resp.ContentLength)
	if err != nil {
		return errorFetchingPostgres(err)
	}

	for _, file := range zipReader.File {
		if !file.FileHeader.FileInfo().IsDir() && strings.HasSuffix(file.FileHeader.Name, ".txz") {
			archiveReader, err := file.Open()
			if err != nil {
				return errorExtractingPostgres(err)
			}

			archiveBytes, err := ioutil.ReadAll(archiveReader)
			if err != nil {
				return errorExtractingPostgres(err)
			}

			cacheLocation, _ := cacheLocator()

			if err := os.MkdirAll(filepath.Dir(cacheLocation), 0o755); err != nil {
				return errorExtractingPostgres(err)
			}

			// It is possible that two processes are doing this at the same time, so write to
			// a temporary file first; then rename it - which is sort-of-mostly atomic.
			tempFile, err := os.CreateTemp(filepath.Dir(cacheLocation), filepath.Base(cacheLocation))
			if err != nil {
				return errorExtractingPostgres(err)
			}
			defer os.Remove(tempFile.Name())

			_, err = tempFile.Write(archiveBytes)
			if err != nil {
				return errorExtractingPostgres(err)
			}

			tempFile.Close()

			// It's possible that another process got here first and so cacheLocation might already
			// exist; so just ignore any error renaming.
			os.Rename(tempFile.Name(), cacheLocation)

			return nil
		}
	}

	return fmt.Errorf("error fetching postgres: cannot find binary in archive retrieved from %s", downloadURL)
}

func errorExtractingPostgres(err error) error {
	return fmt.Errorf("unable to extract postgres archive: %s", err)
}

func errorFetchingPostgres(err error) error {
	return fmt.Errorf("error fetching postgres: %s", err)
}
